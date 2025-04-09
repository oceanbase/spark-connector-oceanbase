/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.reader

import com.oceanbase.spark.reader.OBMySQLLimitPartition
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.util.control.NonFatal

object JDBCLimitRDD extends Logging {

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.
   *
   * @param options
   *   \- JDBC options that contains url, table and other information.
   *
   * @return
   *   A StructType giving the table's Catalyst schema.
   * @throws java.sql.SQLException
   *   if the table specification is garbage.
   * @throws java.sql.SQLException
   *   if the table contains an unsupported type.
   */
  def resolveTable(options: JDBCOptions): StructType = {
    val url = options.url
    val table = options.tableOrQuery
    val dialect = JdbcDialects.get(url)
    getQueryOutputSchema(dialect.getSchemaQuery(table), options, dialect)
  }

  private def getQueryOutputSchema(
      query: String,
      options: JDBCOptions,
      dialect: JdbcDialect): StructType = {
    val conn: Connection = OBJdbcUtils.getConnection(options)
    try {
      val statement = conn.prepareStatement(query)
      try {
        statement.setQueryTimeout(options.queryTimeout)
        val rs = statement.executeQuery()
        try {
          getSchema(rs, dialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param schema
   *   \- The Catalyst schema of the master table
   * @param columns
   *   \- The list of desired columns
   *
   * @return
   *   A Catalyst schema corresponding to columns in the given order.
   */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  /**
   * Turns a single Filter into a String representing a SQL expression. Returns None for an
   * unhandled filter.
   */
  def compileFilter(f: Filter, dialect: JdbcDialect): Option[String] = {
    def quote(colName: String): String = dialect.quoteIdentifier(colName)

    Option(f match {
      case EqualTo(attr, value) => s"${quote(attr)} = ${dialect.compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${dialect.compileValue(value)} OR $col IS NULL OR " +
          s"${dialect.compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${dialect.compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${dialect.compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${dialect.compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${dialect.compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${dialect.compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '$value%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%$value'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%$value%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${dialect.compileValue(value)})"
      case Not(f) => compileFilter(f, dialect).map(p => s"(NOT ($p))").orNull
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  /**
   * Build and return JDBCRDD from the given information.
   *
   * @param sc
   *   \- Your SparkContext.
   * @param schema
   *   \- The Catalyst schema of the underlying database table.
   * @param requiredColumns
   *   \- The names of the columns or aggregate columns to SELECT.
   * @param filters
   *   \- The filters to include in all WHERE clauses.
   * @param parts
   *   \- An array of JDBCPartitions specifying partition ids and per-partition WHERE clauses.
   * @param options
   *   \- JDBC options that contains url, table and other information.
   * @param outputSchema
   *   \- The schema of the columns or aggregate columns to SELECT.
   * @param groupByColumns
   *   \- The pushed down group by columns.
   *
   * @return
   *   An RDD representing "SELECT requiredColumns FROM fqTable".
   */
  def scanTable(
      sc: SparkContext,
      schema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      parts: Array[Partition],
      options: JDBCOptions,
      outputSchema: Option[StructType] = None,
      groupByColumns: Option[Array[String]] = None): RDD[InternalRow] = {
    val url = options.url
    val dialect = JdbcDialects.get(url)
    val quotedColumns = if (groupByColumns.isEmpty) {
      requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    } else {
      // these are already quoted in JDBCScanBuilder
      requiredColumns
    }
    new JDBCLimitRDD(
      sc,
      outputSchema.getOrElse(pruneSchema(schema, requiredColumns)),
      quotedColumns,
      filters,
      parts,
      url,
      options,
      groupByColumns)
  }

  /**
   * Copy from
   * [[org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema(ResultSet, JdbcDialect, Boolean)]]
   * to solve compatibility issues with lower Spark versions.
   *
   * @note
   *   This is a temporary solution pending BATCH_READ refactoring.
   * @param resultSet
   *   JDBC ResultSet containing metadata
   * @param dialect
   *   Spark JdbcDialect implementation
   * @param alwaysNullable
   *   If true, forces all columns to be nullable
   * @return
   *   Catalyst schema (StructType) representation
   * @see
   *   [[org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils]]
   */
  def getSchema(
      resultSet: ResultSet,
      dialect: JdbcDialect,
      alwaysNullable: Boolean = false): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned =
        try rsmd.isSigned(i + 1)
        catch {
          // Workaround for HIVE-14684:
          case e: SQLException
              if e.getMessage == "Method not supported" &&
                rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" =>
            true
        }
      val nullable =
        if (alwaysNullable) true else rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val metadata = new MetadataBuilder()
      metadata.putLong("scale", fieldScale)

      dataType match {
        case java.sql.Types.TIME =>
          // SPARK-33888
          // - include TIME type metadata
          // - always build the metadata
          metadata.putBoolean("logical_time_type", true)
        case java.sql.Types.ROWID =>
          metadata.putBoolean("rowid", true)
        case _ =>
      }

      val columnType =
        dialect
          .getCatalystType(dataType, typeName, fieldSize, metadata)
          .getOrElse(getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    new StructType(fields)
  }

  /**
   * Maps a JDBC type to a Catalyst type. This function is called only when the JdbcDialect class
   * corresponding to your database driver returns null.
   *
   * @param sqlType
   *   \- A field of java.sql.Types
   * @return
   *   The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT =>
        if (signed) { LongType }
        else { DecimalType(20, 0) }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 =>
        DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER =>
        if (signed) { IntegerType }
        else { LongType }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 =>
        DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => StringType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new RuntimeException(s"Unsupported type: $sqlType ")
      // scalastyle:on
    }

    if (answer == null) {
      throw new RuntimeException(s"Unsupported type: $sqlType ")
    }
    answer
  }
}

/**
 * An RDD representing a query is related to a table in a database accessed via JDBC. Both the
 * driver code and the workers must be able to access the database; the driver needs to fetch the
 * schema while the workers need to fetch the data.
 */
class JDBCLimitRDD(
    sc: SparkContext,
    schema: StructType,
    columns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    url: String,
    options: JDBCOptions,
    groupByColumns: Option[Array[String]])
  extends RDD[InternalRow](sc, Nil) {

  /** Retrieve the list of partitions corresponding to this RDD. */
  override def getPartitions: Array[Partition] = partitions

  /** `columns`, but as a String suitable for injection into a SQL query. */
  private val columnList: String = if (columns.isEmpty) "1" else columns.mkString(",")

  /** `filters`, but as a WHERE clause suitable for injection into a SQL query. */
  private val filterWhereClause: String =
    filters
      .flatMap(JDBCLimitRDD.compileFilter(_, JdbcDialects.get(url)))
      .map(p => s"($p)")
      .mkString(" AND ")

  /** A WHERE clause representing both `filters`, if any, and the current partition. */
  private def getWhereClause(part: OBMySQLLimitPartition): String = {
    if (filterWhereClause.nonEmpty) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  /** A GROUP BY clause representing pushed-down grouping columns. */
  private def getGroupByClause: String = {
    if (groupByColumns.nonEmpty && groupByColumns.get.nonEmpty) {
      // The GROUP BY columns should already be quoted by the caller side.
      s"GROUP BY ${groupByColumns.get.mkString(", ")}"
    } else {
      ""
    }
  }

  /** Runs the SQL query against the JDBC driver. */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    val conn = OBJdbcUtils.getConnection(options)

    def close(): Unit = {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => logWarning("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
      closed = true
    }

    context.addTaskCompletionListener[Unit](context => close())

    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[OBMySQLLimitPartition]
    val dialect = JdbcDialects.get(url)
    import scala.collection.JavaConverters._
    dialect.beforeFetch(conn, options.asProperties.asScala.toMap)

    // This executes a generic SQL statement (or PL/SQL block) before reading
    // the table/query via JDBC. Use this feature to initialize the database
    // session environment, e.g. for optimizations and/or troubleshooting.
    options.sessionInitStatement match {
      case Some(sql) =>
        val statement = conn.prepareStatement(sql)
        logInfo(s"Executing sessionInitStatement: $sql")
        try {
          statement.setQueryTimeout(options.queryTimeout)
          statement.execute()
        } finally {
          statement.close()
        }
      case None =>
    }

    val myWhereClause = getWhereClause(part)

    val sqlText =
      s"SELECT $columnList FROM ${options.tableOrQuery} ${part.partitionClause} $myWhereClause" +
        s" $getGroupByClause ${part.limitOffsetClause}"
    stmt = conn.prepareStatement(sqlText, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    rs = stmt.executeQuery()
    val rowsIterator =
      JdbcUtils.resultSetToSparkInternalRows(rs, dialect = dialect, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator),
      close())
  }
}
