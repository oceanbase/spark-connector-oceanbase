# 实现计划：在 spark-connector-oceanbase 中新增 OBKV 读写通道

## Context

当前 `spark-connector-oceanbase` 模块仅支持 JDBC 方式读写 OceanBase，以及 DirectLoad 方式的高性能写入。用户希望新增 OBKV（基于 obkv-table-client-java）作为第三种可选的读写通道，直接通过 OceanBase KV 协议访问普通关系表，绕过 SQL 层以获得更高性能。

**设计模式**：参考现有 DirectLoad 的集成方式——通过配置开关 `obkv.enabled` 切换通道，在 `OceanBaseTable` 的 `newScanBuilder()` 和 `newWriteBuilder()` 中增加 OBKV 分支。

**两种 Schema 获取方式**：
- **Catalog 模式**（Spark 3.x）：Schema 仍通过 JDBC 获取（`OceanBaseCatalog.resolveTable()`），数据读写走 OBKV
- **非 Catalog 模式**：用户通过 `SchemaRelationProvider` + StructType 显式声明 Schema（参考 PR #93 模式）

---

## Step 1: 新增 OBKV 配置项

**文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/java/com/oceanbase/spark/config/OceanBaseConfig.java`

在 `OceanBaseConfig` 中新增 `obkv.*` 前缀的配置项（与 `direct-load.*` 平级）：

```java
// ======== OBKV Related =========
obkv.enabled            // Boolean, 默认 false，是否启用 OBKV 读写
obkv.param-url           // String, OB 配置服务器 URL（直连模式必需）
obkv.full-user-name      // String, 格式: user@tenant#cluster
obkv.password            // String, OBKV 密码（可选，默认复用 password）
obkv.sys-user-name       // String, 系统租户用户名（可选）
obkv.sys-password        // String, 系统租户密码（可选）
obkv.odp-mode            // Boolean, 默认 false，是否通过 ODP 代理
obkv.odp-addr            // String, ODP 地址
obkv.odp-port            // Integer, ODP 端口，默认 2882
obkv.batch-size          // Integer, 批量读写大小，默认 1024
obkv.rpc-execute-timeout // Integer, RPC 执行超时(ms)，默认 3000
obkv.operation-timeout   // Integer, 操作超时(ms)，默认 10000
obkv.dup-action          // String, 写入冲突策略: INSERT_OR_UPDATE(默认)/INSERT/REPLACE/PUT
obkv.read-consistency    // String, 读一致性: STRONG(默认)/WEAK
obkv.primary-key         // String, 主键列名（非 Catalog 模式必需），逗号分隔
```

新增对应的 getter 方法。

---

## Step 2: OBKV 客户端管理工具类

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/java/com/oceanbase/spark/obkv/OBKVClientUtils.java`

职责：
- 根据 `OceanBaseConfig` 创建并初始化 `ObTableClient`
- 支持直连和 ODP 两种模式
- 提供静态工厂方法 `getClient(config): ObTableClient`
- 客户端需要 `Serializable`，在 Executor 端延迟初始化

```java
public class OBKVClientUtils {
    public static ObTableClient createClient(OceanBaseConfig config) {
        ObTableClient client = new ObTableClient();
        client.setFullUserName(config.getObkvFullUserName());
        client.setPassword(config.getObkvPassword());

        if (config.getObkvOdpMode()) {
            client.setOdpAddr(config.getObkvOdpAddr());
            client.setOdpPort(config.getObkvOdpPort());
        } else {
            client.setParamURL(config.getObkvParamUrl());
            if (config.getObkvSysUserName() != null) {
                client.setSysUserName(config.getObkvSysUserName());
                client.setSysPassword(config.getObkvSysPassword());
            }
        }

        // 设置超时等属性
        client.addProperty("rpc.execute.timeout", String.valueOf(config.getObkvRpcExecuteTimeout()));
        client.init();
        return client;
    }
}
```

---

## Step 3: OBKV 分区实现（读取分区）

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/java/com/oceanbase/spark/reader/v2/OBKVPartition.java`

基于 OB 表分区信息实现 `InputPartition`：

```java
public class OBKVPartition implements InputPartition, Serializable {
    private final long partitionId;
    private final Object[] scanRangeStart;  // 分区扫描起始范围
    private final Object[] scanRangeEnd;    // 分区扫描结束范围
    private final boolean startInclusive;
    private final boolean endInclusive;

    // 用于非分区表的全表扫描
    public static OBKVPartition fullScan() { ... }
}
```

分区获取逻辑：
- 调用 `ObTableClient.getPartition(tableName, false)` 获取所有分区
- 对于非分区表，返回单个 `fullScan()` 分区
- 对于分区表（RANGE/LIST/KEY），每个 OB 分区映射为一个 Spark InputPartition

---

## Step 4: OBKV 谓词下推 & Filter 编译

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/java/com/oceanbase/spark/obkv/OBKVFilterCompiler.java`

将 Spark Filter 转换为 OBKV 的查询条件：

```java
public class OBKVFilterCompiler {
    // 主键列集合
    private final Set<String> primaryKeyColumns;

    // 编译结果
    public static class CompileResult {
        List<ScanRange> scanRanges;     // 主键条件 → scan range
        ObTableFilter serverFilter;      // 非主键条件 → ObTableFilter
        List<Filter> unhandledFilters;   // 无法下推的条件
    }

    public CompileResult compile(Filter[] filters) {
        // 1. 分离主键条件和非主键条件
        // 2. 主键的 EqualTo/GreaterThan/LessThan 等 → ScanRange
        // 3. 非主键的 EqualTo/GreaterThan 等 → ObTableValueFilter
        // 4. And/Or → ObTableFilterList
        // 5. 不支持的 → unhandledFilters（由 Spark 端过滤）
    }
}
```

支持的下推映射：
| Spark Filter | OBKV 目标 |
|---|---|
| `EqualTo(pk_col, val)` | `addScanRange(val, true, val, true)` |
| `GreaterThan(pk_col, val)` | `addScanRange(val, false, MAX, true)` |
| `GreaterThanOrEqual(pk_col, val)` | `addScanRange(val, true, MAX, true)` |
| `LessThan(pk_col, val)` | `addScanRange(MIN, true, val, false)` |
| `EqualTo(col, val)` | `ObTableValueFilter(EQ, col, val)` |
| `GreaterThan(col, val)` | `ObTableValueFilter(GT, col, val)` |
| `And(f1, f2)` | `ObTableFilterList(AND, f1, f2)` |
| `Or(f1, f2)` | `ObTableFilterList(OR, f1, f2)` |
| `IsNull(col)` | `ObTableValueFilter(IS, col, null)` |

---

## Step 5: OBKV 读取链路（V2 API）

### 5.1 ScanBuilder

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/reader/v2/OBKVScanBuilder.scala`

```scala
case class OBKVScanBuilder(schema: StructType, config: OceanBaseConfig, primaryKeys: Array[String])
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var finalSchema = schema
  private var pushedFilters = Array.empty[Filter]

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val result = OBKVFilterCompiler.compile(filters, primaryKeys)
    pushedFilters = filters.diff(result.unhandledFilters)
    result.unhandledFilters
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    finalSchema = requiredSchema
  }

  override def build(): Scan = OBKVBatchScan(finalSchema, config, pushedFilters, primaryKeys)
}
```

### 5.2 Batch + ReaderFactory

```scala
case class OBKVBatchScan(schema: StructType, config: OceanBaseConfig,
                          pushedFilters: Array[Filter], primaryKeys: Array[String])
  extends Scan with Batch {

  override def readSchema(): StructType = schema
  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // 使用 ObTableClient 获取分区信息
    val client = OBKVClientUtils.createClient(config)
    try {
      val partitions = client.getPartition(config.getTableName, false)
      if (partitions == null || partitions.isEmpty) {
        Array(OBKVPartition.fullScan())
      } else {
        partitions.map(p => new OBKVPartition(p)).toArray
      }
    } finally {
      client.close()
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new OBKVReaderFactory(schema, config, pushedFilters, primaryKeys)
}
```

### 5.3 PartitionReader

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/reader/v2/OBKVReader.scala`

```scala
class OBKVReader(schema: StructType, config: OceanBaseConfig,
                 partition: OBKVPartition, pushedFilters: Array[Filter],
                 primaryKeys: Array[String])
  extends PartitionReader[InternalRow] {

  // 延迟初始化
  private lazy val client = OBKVClientUtils.createClient(config)
  private lazy val resultSet: QueryResultSet = {
    val query = client.query(config.getTableName)

    // 1. 设置列选择
    query.select(schema.fieldNames: _*)

    // 2. 设置扫描范围（来自分区 + 主键谓词）
    partition.applyScanRange(query)

    // 3. 设置服务端过滤器
    val compiled = OBKVFilterCompiler.compile(pushedFilters, primaryKeys)
    if (compiled.serverFilter != null) {
      query.setFilter(compiled.serverFilter)
    }

    // 4. 设置流式批量大小
    query.setBatchSize(config.getObkvBatchSize)

    // 5. 设置读一致性
    if (config.getObkvReadConsistency == "WEAK") {
      query.setReadConsistency(ObReadConsistency.WEAK)
    }

    query.execute()
  }

  // 类型转换：OBKV Object → Spark InternalRow
  private val mutableRow = new SpecificInternalRow(schema.fields.map(_.dataType))

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = {
    val row = resultSet.getRow()  // Map<String, Object>
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val value = row.get(field.name)
        if (value == null) mutableRow.setNullAt(i)
        else OBKVTypeConverter.setValue(mutableRow, i, field.dataType, value)
    }
    mutableRow
  }

  override def close(): Unit = {
    if (client != null) client.close()
  }
}
```

### 5.4 类型转换器

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/java/com/oceanbase/spark/obkv/OBKVTypeConverter.java`

OBKV 返回 Java 对象 → Spark InternalRow 的类型映射：

|    OBKV Java 类型    | Spark DataType |                转换方式                 |
|--------------------|----------------|-------------------------------------|
| `byte/Byte`        | ByteType       | 直接                                  |
| `short/Short`      | ShortType      | 直接                                  |
| `int/Integer`      | IntegerType    | 直接                                  |
| `long/Long`        | LongType       | 直接                                  |
| `float/Float`      | FloatType      | 直接                                  |
| `double/Double`    | DoubleType     | 直接                                  |
| `boolean/Boolean`  | BooleanType    | 直接                                  |
| `String`           | StringType     | `UTF8String.fromString()`           |
| `BigDecimal`       | DecimalType    | `Decimal.apply()`                   |
| `Timestamp`        | TimestampType  | `DateTimeUtils.fromJavaTimestamp()` |
| `Date / LocalDate` | DateType       | `DateTimeUtils.fromJavaDate()`      |
| `byte[]`           | BinaryType     | 直接                                  |

反向（Spark → OBKV Java 对象）用于写入时的值转换。

---

## Step 6: OBKV 写入链路（V2 API）

### 6.1 WriteBuilder

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/writer/v2/OBKVWriteBuilder.scala`

```scala
class OBKVWriteBuilder(schema: StructType, config: OceanBaseConfig)
  extends WriteBuilder with SupportsTruncate {

  override def build(): Write = new OBKVWrite(schema, config)

  override def truncate(): WriteBuilder = {
    // 通过 JDBC 执行 TRUNCATE（OBKV 不支持 DDL）
    OBJdbcUtils.truncateTable(config)
    this
  }
}
```

### 6.2 BatchWrite + Writer

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/writer/v2/OBKVWriter.scala`

```scala
class OBKVWriter(schema: StructType, config: OceanBaseConfig)
  extends DataWriter[InternalRow] {

  private val batchSize = config.getObkvBatchSize
  private val buffer = ArrayBuffer[InternalRow]()
  private lazy val client = OBKVClientUtils.createClient(config)
  private val dupAction = config.getObkvDupAction  // INSERT_OR_UPDATE / INSERT / REPLACE / PUT

  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.length >= batchSize) flush()
  }

  private def flush(): Unit = {
    if (buffer.isEmpty) return
    val batchOps = client.batch(config.getTableName)

    buffer.foreach { row =>
      val (rowKey, columns, values) = extractRowData(schema, row)
      dupAction match {
        case "INSERT_OR_UPDATE" => batchOps.insertOrUpdate(rowKey, columns, values)
        case "INSERT" => batchOps.insert(rowKey, columns, values)
        case "REPLACE" => batchOps.replace(rowKey, columns, values)
        case "PUT" => batchOps.put(rowKey, columns, values)
      }
    }
    batchOps.execute()
    buffer.clear()
  }

  override def commit(): WriterCommitMessage = { flush(); CommitMessage() }
  override def close(): Unit = { if (client != null) client.close() }
}
```

---

## Step 7: 路由集成（OceanBaseTable 修改）

**修改文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/catalog/OceanBaseTable.scala`

```scala
// 在 newScanBuilder 中添加 OBKV 分支
override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
  if (config.getObkvEnabled) {
    // Catalog 模式下，主键信息通过 JDBC 获取
    val primaryKeys = OBJdbcUtils.withConnection(config) { conn =>
      dialect.getPriKeyInfo(conn, config.getSchemaName, config.getTableName, config)
        .map(_.columnName).toArray
    }
    OBKVScanBuilder(schema, config, primaryKeys)
  } else {
    // 原有 JDBC 逻辑不变
    ...
  }
}

// 在 newWriteBuilder 中添加 OBKV 分支
override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
  if (config.getObkvEnabled) {
    new OBKVWriteBuilder(schema, config)
  } else if (config.getDirectLoadEnable) {
    DirectLoadWriteBuilderV2(schema, config)
  } else {
    new JDBCWriteBuilder(schema, config, dialect)
  }
}
```

---

## Step 8: 非 Catalog 模式（V1 DataSource）

**修改文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/org/apache/spark/sql/OceanBaseSparkDataSource.scala`

添加 `SchemaRelationProvider` 接口支持：

```scala
class OceanBaseSparkDataSource extends JdbcRelationProvider with SchemaRelationProvider {

  // 新增：用户显式声明 schema 的入口
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val config = new OceanBaseConfig(parameters.asJava)
    if (config.getObkvEnabled) {
      // 解析用户指定的主键列
      val primaryKeys = config.getObkvPrimaryKey.split(",").map(_.trim)
      OBKVRelation(parameters, Some(schema), primaryKeys)(sqlContext)
    } else {
      // 回退到原有 JDBC 逻辑
      super.createRelation(sqlContext, parameters)
    }
  }

  // 修改现有的写入方法，添加 OBKV 分支
  override def createRelation(
      sqlContext: SQLContext, mode: SaveMode,
      parameters: Map[String, String], dataFrame: DataFrame): BaseRelation = {
    val config = new OceanBaseConfig(parameters.asJava)
    if (config.getObkvEnabled) {
      // OBKV 写入
      writeDataViaObkv(mode, dataFrame, config)
      createRelation(sqlContext, parameters)
    } else if (config.getDirectLoadEnable) {
      writeDataViaDirectLoad(mode, dataFrame, config)
      createRelation(sqlContext, parameters)
    } else {
      super.createRelation(sqlContext, mode, parameters, dataFrame)
    }
  }
}
```

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/main/scala/com/oceanbase/spark/OBKVRelation.scala`

```scala
case class OBKVRelation(
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    primaryKeys: Array[String]
)(val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def schema: StructType = userSpecifiedSchema.get

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // 构建 OBKVRDD 实现并行读取
    new OBKVRDD(sqlContext.sparkContext, config, schema, requiredColumns, filters, primaryKeys)
  }

  override def insert( DataFrame, overwrite: Boolean): Unit = {
    // OBKV 批量写入
  }
}
```

---

## Step 9: Maven 依赖配置

**修改文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/pom.xml`

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>obkv-table-client</artifactId>
    <version>2.3.0</version>
</dependency>
```

注意：当前项目已依赖 `obkv-table-client` v2.0.0，需要升级到 v2.3.0 以获取更完善的 `getPartition()` API。

---

## Step 10: 测试

### 单元测试

**新建文件**: `spark-connector-oceanbase/spark-connector-oceanbase-base/src/test/scala/com/oceanbase/spark/obkv/OBKVFilterCompilerTest.scala`
- 测试 Filter 编译逻辑

### 集成测试

**新建文件**: `spark-connector-oceanbase-e2e-tests/src/test/java/com/oceanbase/spark/OBKVConnectorITCase.java`
- 测试 OBKV 读写端到端流程
- 测试 Catalog 模式 + OBKV 读写
- 测试非 Catalog 模式 + 用户声明 Schema
- 测试谓词下推效果

---

## 关键文件清单

### 新建文件

|                 文件                 |               说明                |
|------------------------------------|---------------------------------|
| `obkv/OBKVClientUtils.java`        | OBKV 客户端工厂                      |
| `obkv/OBKVFilterCompiler.java`     | 谓词编译器                           |
| `obkv/OBKVTypeConverter.java`      | 类型转换器                           |
| `reader/v2/OBKVPartition.java`     | OBKV 分区实现                       |
| `reader/v2/OBKVScanBuilder.scala`  | V2 读取构建器（含 Batch/ReaderFactory） |
| `reader/v2/OBKVReader.scala`       | V2 分区读取器                        |
| `writer/v2/OBKVWriteBuilder.scala` | V2 写入构建器                        |
| `writer/v2/OBKVWriter.scala`       | V2 写入器                          |
| `OBKVRelation.scala`               | V1 非 Catalog 关系（含 OBKVRDD）      |

### 修改文件

|                文件                |                    修改内容                     |
|----------------------------------|---------------------------------------------|
| `config/OceanBaseConfig.java`    | 新增 `obkv.*` 配置项                             |
| `catalog/OceanBaseTable.scala`   | `newScanBuilder/newWriteBuilder` 增加 OBKV 分支 |
| `OceanBaseSparkDataSource.scala` | 添加 `SchemaRelationProvider`，增加 OBKV 读写分支    |
| `pom.xml`                        | 升级 obkv-table-client 依赖版本                   |

### 参考复用的现有代码

|               文件               |          复用内容          |
|--------------------------------|------------------------|
| `ExprUtils.scala`              | Filter 编译模式参考          |
| `OBJdbcReader.scala:272-468`   | `makeGetters` 类型转换模式参考 |
| `JDBCWriter.scala:34-150`      | 批量缓冲写入模式参考             |
| `JDBCWriteBuilder.scala:31-57` | WriteBuilder 结构参考      |
| `OBJdbcScanBuilder.scala`      | ScanBuilder + 下推接口参考   |
| `OBJdbcUtils.java`             | JDBC 工具方法（获取主键信息等）     |

---

## 建议实现顺序

1. **Step 1**: 配置项（基础，无依赖）
2. **Step 9**: Maven 依赖（基础）
3. **Step 2**: 客户端工具类（依赖 Step 1）
4. **Step 5.4**: 类型转换器（独立）
5. **Step 4**: 谓词编译器（独立）
6. **Step 3**: 分区实现（依赖 Step 2）
7. **Step 6**: 写入链路（依赖 Step 2, 5.4）
8. **Step 5**: 读取链路（依赖 Step 2-4, 5.4）
9. **Step 7**: Catalog 集成（依赖 Step 5, 6）
10. **Step 8**: 非 Catalog 模式（依赖 Step 5, 6）
11. **Step 10**: 测试

---

## 验证方式

1. **编译验证**: `mvn clean compile -pl spark-connector-oceanbase -am`
2. **单元测试**: `mvn test -pl spark-connector-oceanbase/spark-connector-oceanbase-base -Dtest=OBKVFilterCompilerTest`
3. **E2E 测试**: 启动 OceanBase Testcontainer，运行集成测试：

   ```
   mvn verify -pl spark-connector-oceanbase-e2e-tests \
     -Dtest=OBKVConnectorITCase \
     -Dspark_version=3.4
   ```
4. **手动验证**:

   ```sql
   -- Catalog 模式
   spark.conf.set("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
   spark.conf.set("spark.sql.catalog.ob.url", "jdbc:oceanbase://host:port/db")
   spark.conf.set("spark.sql.catalog.ob.obkv.enabled", "true")
   spark.conf.set("spark.sql.catalog.ob.obkv.param-url", "http://config_server")
   spark.conf.set("spark.sql.catalog.ob.obkv.full-user-name", "user@tenant#cluster")
   spark.sql("SELECT * FROM ob.db.table WHERE id > 100")

   -- 非 Catalog 模式（用户声明 Schema）
   CREATE TEMPORARY VIEW t (id INT, name STRING, age INT)
   USING oceanbase OPTIONS(
     'obkv.enabled'='true',
     'obkv.param-url'='http://config_server',
     'obkv.full-user-name'='user@tenant#cluster',
     'obkv.password'='xxx',
     'table-name'='test_table',
     'obkv.primary-key'='id'
   );
   SELECT * FROM t WHERE id = 1;
   INSERT INTO t VALUES (1, 'test', 25);
   ```

---

## 技术风险与注意事项

1. **Schema 获取**：OBKV 无法获取列 Schema，Catalog 模式下仍依赖 JDBC 连接（用户需同时配置 url 和 obkv.* 参数）
2. **分区映射**：`ObTableClient.getPartition()` 返回的分区范围信息是否能精确映射为 scan range，需要根据分区类型（KEY/RANGE/LIST）分别处理
3. **类型精度**：OBKV 返回的 Java 对象类型可能与 JDBC 不完全一致，需要在 OBKVTypeConverter 中仔细处理
4. **序列化**：`OceanBaseConfig` 已实现 `Serializable`，但 `ObTableClient` 不可序列化，需要在 Executor 端延迟初始化
5. **DDL 操作**：OBKV 不支持 DDL，Catalog 的 createTable/dropTable/alterTable 仍走 JDBC
6. **obkv-table-client 版本**：确认 `getPartition()` API 在目标版本中可用

