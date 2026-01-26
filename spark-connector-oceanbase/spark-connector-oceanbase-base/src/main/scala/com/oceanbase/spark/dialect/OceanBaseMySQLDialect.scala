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

package com.oceanbase.spark.dialect

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils
import com.oceanbase.spark.utils.OBJdbcUtils.executeStatement

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.ExprUtils
import org.apache.spark.sql.connector.expressions.{Expression, NullOrdering, SortDirection, Transform}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, MetadataBuilder, ShortType, StringType, StructType, TimestampType, VarcharType}

import java.sql.{Connection, Types}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal;

class OceanBaseMySQLDialect extends OceanBaseDialect {

  def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      partitions: Array[Transform],
      config: OceanBaseConfig,
      properties: java.util.Map[String, String]): Unit = {

    def buildCreateTableSQL(
        tableName: String,
        schema: StructType,
        transforms: Array[Transform],
        config: OceanBaseConfig): String = {
      val partitionClause = buildPartitionClause(transforms, config)
      val columnClause = schema.fields
        .map {
          field =>
            val obType = toOceanBaseMySQLType(field.dataType, config)
            val nullability = if (field.nullable) StringUtils.EMPTY else "NOT NULL"
            val comment = field.getComment() match {
              case Some(v) => s"COMMENT '$v'"
              case _ => StringUtils.EMPTY
            }
            // TODO: support default value
            s"${quoteIdentifier(field.name)} $obType $nullability $comment".trim
        }
        .mkString(",\n  ")
      val tableComment = Option(config.getTableComment) match {
        case Some(comment) => s"COMMENT '$comment'"
        case _ => StringUtils.EMPTY
      }
      var primaryKey = ""
      val tableOption = properties.asScala
        .map(tuple => (tuple._1.toLowerCase, tuple._2))
        .flatMap {
          case ("charset", value) => Some(s"DEFAULT CHARSET = $value")
          case ("collate", value) => Some(s"COLLATE = $value")
          case ("primary_zone", value) => Some(s"PRIMARY_ZONE = '$value'")
          case ("replica_num", value) => Some(s"REPLICA_NUM = $value")
          case ("compression", value) => Some(s"COMPRESSION = '$value'")
          case ("primary_key", value) =>
            primaryKey = s", PRIMARY KEY($value)"
            None
          case (k, _) =>
            logWarning(s"Ignored unsupported table property: $k")
            None
        }
        .mkString(" ", " ", "")
      s"""
         |CREATE TABLE $tableName (
         |  $columnClause
         |  $primaryKey
         |) $tableOption $tableComment
         |$partitionClause;
         |""".stripMargin.trim
    }

    def toOceanBaseMySQLType(dataType: DataType, config: OceanBaseConfig): String = {
      var stringConvertType = s"VARCHAR(${config.getLengthString2Varchar})"
      if (config.getEnableString2Text) stringConvertType = "TEXT"
      dataType match {
        case BooleanType => "BOOLEAN"
        case ByteType => "BYTE"
        case ShortType => "SMALLINT"
        case IntegerType => "INT"
        case LongType => "BIGINT"
        case FloatType => "FLOAT"
        case DoubleType => "DOUBLE"
        case d: DecimalType => s"DECIMAL(${d.precision},${d.scale})"
        case c: CharType => s"CHAR(${c.length})"
        case v: VarcharType => s"VARCHAR(${v.length})"
        case StringType => stringConvertType
        case BinaryType => "BINARY"
        case DateType => "DATE"
        case TimestampType => "DATETIME"
        // TODO: Support array data-type
        case _ => throw new UnsupportedOperationException(s"Unsupported type: $dataType")
      }
    }

    def buildPartitionClause(transforms: Array[Transform], config: OceanBaseConfig): String = {
      transforms match {
        case transforms if transforms.nonEmpty =>
          ExprUtils.toOBMySQLPartition(transforms.head, config)
        case _ => ""
      }
    }

    val sql = buildCreateTableSQL(tableName, schema, partitions, config)
    executeStatement(conn, config, sql)
  }

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      comment: String): Unit = {
    // OceanBase mysql mode does not support schema comments, so we ignore the comment parameter.
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(config.getJdbcQueryTimeout)
      statement.executeUpdate(s"CREATE SCHEMA ${quoteIdentifier(schema)}")
    } finally {
      statement.close()
    }
  }

  override def schemaExists(conn: Connection, config: OceanBaseConfig, schema: String): Boolean = {
    listSchemas(conn, config).exists(_.head == schema)
  }

  override def listSchemas(conn: Connection, config: OceanBaseConfig): Array[Array[String]] = {
    val schemaBuilder = mutable.ArrayBuilder.make[Array[String]]
    try {
      OBJdbcUtils.executeQuery(conn, config, "SHOW SCHEMAS") {
        rs =>
          while (rs.next()) {
            schemaBuilder += Array(rs.getString("Database"))
          }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot show schemas.")
    }
    schemaBuilder.result
  }

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      cascade: Boolean): Unit = {
    executeStatement(
      conn,
      config,
      if (cascade) {
        s"DROP SCHEMA ${quoteIdentifier(schema)} CASCADE"
      } else {
        s"DROP SCHEMA ${quoteIdentifier(schema)}"
      })
  }

  def getPriKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    val sql =
      s"""
         |select
         |  COLUMN_NAME, COLUMN_TYPE , COLUMN_KEY, DATA_TYPE, EXTRA
         |from
         |  information_schema.columns
         |where
         |      TABLE_SCHEMA = '$schemaName'
         |  and TABLE_NAME = '$tableName';
         |""".stripMargin

    val arrayBuffer = ArrayBuffer[PriKeyColumnInfo]()
    OBJdbcUtils.executeQuery(connection, config, sql) {
      rs =>
        {
          while (rs.next()) {
            val columnKey = rs.getString(3)
            if (null != columnKey && columnKey.equals("PRI")) {
              arrayBuffer += PriKeyColumnInfo(
                quoteIdentifier(rs.getString(1)),
                rs.getString(2),
                columnKey,
                rs.getString(4),
                rs.getString(5))
            }
          }
          arrayBuffer
        }
    }
  }

  def getUniqueKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    val sql =
      s"""
         |show index from ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)} where non_unique = 0 AND key_name <> 'PRIMARY';
         |""".stripMargin
    val arrayBuffer = ArrayBuffer[PriKeyColumnInfo]()
    OBJdbcUtils.executeQuery(connection, config, sql) {
      rs =>
        {
          while (rs.next()) {
            arrayBuffer += PriKeyColumnInfo(
              quoteIdentifier(rs.getString("Column_name")),
              StringUtils.EMPTY,
              StringUtils.EMPTY,
              StringUtils.EMPTY,
              StringUtils.EMPTY)
          }
          arrayBuffer
        }
    }
  }

  def getInsertIntoStatement(
      tableName: String,
      schema: StructType,
      config: OceanBaseConfig): String = {
    val columnClause =
      schema.fieldNames.map(columnName => quoteIdentifier(columnName)).mkString(", ")
    val placeholders = schema.fieldNames.map(_ => "?").mkString(", ")

    val hints = config.getJdbcWriteHintsPushdown match {
      case hint if hint.trim.nonEmpty => s"/*+ $hint */"
      case _ => OceanBaseConfig.EMPTY_STRING
    }

    s"""
       |INSERT $hints INTO $tableName ($columnClause)
       |VALUES ($placeholders)
       |""".stripMargin
  }

  def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo],
      config: OceanBaseConfig): String = {
    val uniqueKeys = priKeyColumnInfo.map(_.columnName).toSet
    val nonUniqueFields =
      schema.fieldNames.filterNot(fieldName => uniqueKeys.contains(quoteIdentifier(fieldName)))

    val hints = config.getJdbcWriteHintsPushdown match {
      case hint if hint.trim.nonEmpty => s"/*+ $hint */"
      case _ => OceanBaseConfig.EMPTY_STRING
    }

    val baseInsert = {
      val columns = schema.fieldNames.map(quoteIdentifier).mkString(", ")
      val placeholders = schema.fieldNames.map(_ => "?").mkString(", ")
      s"INSERT $hints INTO $tableName ($columns) VALUES ($placeholders)"
    }

    // Force INSERT IGNORE if explicitly configured
    if (config.getJdbcUseInsertIgnore) {
      baseInsert.replace("INSERT", "INSERT IGNORE")
    } else if (nonUniqueFields.nonEmpty) {
      // ON DUPLICATE KEY UPDATE
      val updateClause = nonUniqueFields
        .map(f => s"${quoteIdentifier(f)} = VALUES(${quoteIdentifier(f)})")
        .mkString(", ")
      s"$baseInsert ON DUPLICATE KEY UPDATE $updateClause"
    } else {
      // INSERT IGNORE (fallback when all columns are keys)
      baseInsert.replace("INSERT", "INSERT IGNORE")
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // See SPARK-35446: MySQL treats REAL as a synonym to DOUBLE by default
    // We override getJDBCType so that FloatType is mapped to FLOAT instead
    case FloatType => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case _ => getCommonJDBCType(dt)
  }

  /**
   * Parse OceanBase element type name to Spark DataType with nested array support. Supports up to 6
   * levels of nesting as per OceanBase limitation.
   *
   * @param elementTypeName
   *   The type name to parse (e.g., "INT", "ARRAY(INT)", "ARRAY(ARRAY(INT))")
   * @param depth
   *   Current nesting depth (starts from 0)
   * @return
   *   Corresponding Spark DataType
   */
  private def parseElementType(elementTypeName: String, depth: Int = 0): DataType = {
    val upperTypeName = elementTypeName.toUpperCase.trim

    // OceanBase supports maximum 6 levels of array nesting
    // If exceeds limit, treat as StringType
    if (depth >= 6) {
      logWarning(
        s"Array nesting depth exceeds OceanBase limit of 6: $upperTypeName. Treating as StringType.")
      return StringType
    }

    // Check for nested ARRAY type
    if (upperTypeName.startsWith("ARRAY(")) {
      val pattern = """ARRAY\((.+)\)""".r
      pattern.findFirstMatchIn(upperTypeName) match {
        case Some(m) =>
          val innerTypeName = m.group(1)
          // Recursively parse inner type with incremented depth
          val innerType = parseElementType(innerTypeName, depth + 1)
          ArrayType(innerType, containsNull = true)
        case None =>
          logWarning(s"Failed to parse nested array type: $upperTypeName")
          StringType
      }
    } else {
      // Parse basic types
      upperTypeName match {
        case "INT" | "INTEGER" => IntegerType
        case "BIGINT" | "LONG" => LongType
        case "FLOAT" => FloatType
        case "DOUBLE" => DoubleType
        case "BOOLEAN" | "BOOL" => BooleanType
        case "STRING" | "VARCHAR" | "TEXT" => StringType
        case _ =>
          logWarning(s"Unknown element type: $upperTypeName. Treating as StringType.")
          StringType
      }
    }
  }

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    val upperTypeName = typeName.toUpperCase

    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else if (upperTypeName.startsWith("VECTOR(")) {
      // VECTOR(n) where n is the dimension, stored as FLOAT by default
      // Parse VECTOR(3) -> ArrayType(FloatType)
      Option(ArrayType(FloatType, containsNull = true))
    } else if (upperTypeName.startsWith("ARRAY(")) {
      // Parse ARRAY(INT) to get element type
      // Note: OceanBase uses parentheses, not angle brackets
      val pattern = """ARRAY\((.+)\)""".r
      pattern.findFirstMatchIn(upperTypeName) match {
        case Some(m) =>
          val elementTypeName = m.group(1)
          val elementType = parseElementType(elementTypeName)
          Option(ArrayType(elementType, containsNull = true))
        case None =>
          // Fallback to Array of String if parsing fails
          logWarning(s"Failed to parse $upperTypeName, using ArrayType(StringType)")
          Option(ArrayType(StringType, containsNull = true))
      }
    } else if (upperTypeName.startsWith("MAP(")) {
      // Parse MAP(INT,INT) to get key and value types
      // Note: OceanBase uses parentheses, not angle brackets
      // Use non-greedy match and handle potential spaces
      val pattern = """MAP\(([^,]+),\s*([^)]+)\)""".r
      pattern.findFirstMatchIn(upperTypeName) match {
        case Some(m) =>
          val keyTypeName = m.group(1).trim
          val valueTypeName = m.group(2).trim
          val keyType = parseElementType(keyTypeName)
          val valueType = parseElementType(valueTypeName)
          Option(MapType(keyType, valueType, valueContainsNull = true))
        case None =>
          // Fallback to Map of String->String if parsing fails
          Option(MapType(StringType, StringType, valueContainsNull = true))
      }
    } else if (upperTypeName.equals("JSON")) {
      // JSON type is stored as StringType in Spark
      Option(StringType)
    } else if (upperTypeName.startsWith("ENUM(") || upperTypeName.startsWith("SET(")) {
      // ENUM and SET are stored as StringType
      Option(StringType)
    } else {
      None
    }
  }

  private val distinctUnsupportedAggregateFunctions =
    Set("VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP")

  // See https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html
  private val supportedAggregateFunctions =
    Set("MAX", "MIN", "SUM", "COUNT", "AVG") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class MySQLSQLBuilder extends JDBCSQLBuilder {
    override def visitSortOrder(
        sortKey: String,
        sortDirection: SortDirection,
        nullOrdering: NullOrdering): String = {
      (sortDirection, nullOrdering) match {
        case (SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) =>
          s"$sortKey $sortDirection"
        case (SortDirection.ASCENDING, NullOrdering.NULLS_LAST) =>
          s"CASE WHEN $sortKey IS NULL THEN 1 ELSE 0 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_FIRST) =>
          s"CASE WHEN $sortKey IS NULL THEN 0 ELSE 1 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_LAST) =>
          s"$sortKey $sortDirection"
      }
    }

    override def visitStartsWith(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '${escapeSpecialCharsForLikePattern(value)}%' ESCAPE '\\\\'"
    }

    override def visitEndsWith(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '%${escapeSpecialCharsForLikePattern(value)}' ESCAPE '\\\\'"
    }

    override def visitContains(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '%${escapeSpecialCharsForLikePattern(value)}%' ESCAPE '\\\\'"
    }

    override def visitAggregateFunction(
        funcName: String,
        isDistinct: Boolean,
        inputs: Array[String]): String =
      if (isDistinct && distinctUnsupportedAggregateFunctions.contains(funcName)) {
        throw new UnsupportedOperationException(
          s"${this.getClass.getSimpleName} does not " +
            s"support aggregate function: $funcName with DISTINCT");
      } else {
        super.visitAggregateFunction(funcName, isDistinct, inputs)
      }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val mysqlSQLBuilder = new MySQLSQLBuilder()
    try {
      Some(mysqlSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  /**
   * Get actual column type names from information_schema.columns for MySQL mode. Returns
   * COLUMN_TYPE which includes full type definition like "ARRAY(INT)", "VECTOR(3)"
   */
  override def getActualColumnTypes(
      connection: Connection,
      tableName: String,
      schemaName: String,
      config: OceanBaseConfig): Map[String, String] = {
    val sql =
      s"""
         |SELECT COLUMN_NAME, COLUMN_TYPE
         |FROM information_schema.columns
         |WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
         |""".stripMargin

    val stmt = connection.prepareStatement(sql)
    try {
      stmt.setString(1, schemaName)
      stmt.setString(2, tableName)
      val rs = stmt.executeQuery()
      val typeMap = scala.collection.mutable.Map[String, String]()
      while (rs.next()) {
        val columnName = rs.getString("COLUMN_NAME")
        val columnType = rs.getString("COLUMN_TYPE").toUpperCase
        typeMap(columnName.toUpperCase) = columnType
      }
      rs.close()
      typeMap.toMap
    } catch {
      case e: Exception =>
        // Log warning but don't fail - fall back to JDBC metadata
        logWarning(s"Failed to get actual column types from information_schema: ${e.getMessage}")
        Map.empty[String, String]
    } finally {
      stmt.close()
    }
  }
}
