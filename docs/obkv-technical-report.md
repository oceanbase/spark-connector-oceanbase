# Spark Connector OceanBase OBKV 通道技术报告

## 概述

本文档详细阐述 Spark Connector for OceanBase 中 OBKV 通道的实现原理、架构设计、功能特性及局限性。

## 一、三个项目的关系

### 1.1 项目概览

- spark-connector-oceanbase: Spark 连接器，提供 Spark DataSource V2 API
- obkv-table-client-java: OBKV Table 客户端，封装 OBKV Table 协议
- obkv-hbase-client-java: HBase 兼容客户端，提供 HBase API 风格的 OBKV 访问

### 1.2 Maven 依赖

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>obkv-table-client</artifactId>
    <version>2.3.0</version>
</dependency>
```

## 二、三条通道对比

|    特性     |      JDBC       |       Direct Load       |       OBKV        |
|-----------|-----------------|-------------------------|-------------------|
| 协议层       | MySQL/Oracle 协议 | 独立旁路协议                  | KV 协议             |
| 性能        | 中等              | 极高                      | 高                 |
| Oracle 模式 | 支持              | 支持                      | 直连不支持             |
| 配置开关      | 默认              | direct-load.enable=true | obkv.enabled=true |

## 三、OBKV 读取通道

### 3.1 架构

```
OBKVScanBuilder (谓词下推 + 列裁剪)
    ↓
OBKVBatchScan (获取分区信息，并行读取)
    ↓
OBKVReaderFactory
    ↓
OBKVPartition (分区信息)
    ↓
OBKVReader (流式读取)
```

### 3.2 分区并行读取

从 v1.5.0 开始支持基于 OceanBase 分区信息的并行读取：
- 通过 client.getPartition(tableName, false) 获取所有分区
- 为每个分区创建一个 Spark InputPartition
- 支持回退到单分区模式

### 3.3 谓词下推

支持的过滤器：EqualTo, GreaterThan, LessThan, IsNull, IsNotNull, And, Or
不支持的过滤器：Not, In, Like

## 四、OBKV 写入通道

### 4.1 批量写入

```scala
class OBKVWriter extends DataWriter[InternalRow] {
  private val batchSize = config.getObkvBatchSize
  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.length >= batchSize) flush()
  }
}
```

### 4.2 写入操作类型

- insertOrUpdate: 主键存在则更新，不存在则插入（默认）
- put: 直接写入，覆盖已有值
- replace: 替换整行
- insert: 仅插入，主键冲突报错

## 五、配置项

```properties
obkv.enabled=true
obkv.param-url=jdbc:oceanbase://host:port/database
obkv.full-user-name=user@tenant
obkv.password=password
obkv.odp-mode=false
obkv.batch-size=1024
obkv.dup-action=INSERT_OR_UPDATE
obkv.primary-key=id,name
```

## 六、局限性

### 6.1 当前限制

- 过滤器支持不全：不支持 Not, In, Like 等
- Decimal 精度丢失：写入时转 Double
- Oracle 模式不兼容：直连模式依赖 MySQL 系统表
- 类型检查严格：OBKV 对写入数据的类型检查非常严格，Spark SQL 解析数字时默认推断为 INT 类型，可能导致 TINYINT/SMALLINT 列写入失败

### 6.2 Decimal 类型限制

obkv-table-client-java 不支持 BigDecimal 类型：
- ObNumberType 的 encode/decode 方法返回空数组/null
- 当前解决方案：写入时将 Decimal 转为 Double

### 6.3 类型检查限制

OBKV 服务端对写入数据的类型检查非常严格：
- Spark SQL 解析 SQL 语句中的数字字面量时，默认推断为 INT 类型
- 当写入 TINYINT 或 SMALLINT 列时，如果值超出该类型范围但被解析为 INT，会导致类型不匹配错误
- 错误示例：`Column type for 'col_smallint' not match, schema column type is 'SMALLINT', input column type is 'INT'`
- Spark SQL 解析浮点数字面量时，默认推断为 DOUBLE 类型
- 当写入 FLOAT 列时，会导致类型不匹配错误
- 错误示例：`Column type for 'col_float' not match, schema column type is 'FLOAT', input column type is 'DOUBLE'`
- 建议使用 DataFrame API 显式指定类型，或通过 JDBC 写入边界值

**解决方案：**
1. 对于 TINYINT、SMALLINT 和 FLOAT 列，避免使用 SQL INSERT 语句写入，改用 DataFrame API 并显式指定列类型
2. 或者使用 JDBC 通道写入这些类型的数据
3. 在设计表结构时，考虑使用 INT 类型替代 TINYINT 和 SMALLINT，使用 DOUBLE 类型替代 FLOAT

### 6.4 字符串写入类型转换验证

**验证结论：不支持通过字符串写入其他类型列**

经过两轮实际测试验证，无论是使用 Catalog 方式还是声明式 Schema 方式，都无法通过字符串写入非字符串类型的列。

#### 6.4.1 使用 Catalog 方式（Spark 类型检查）

**测试结果：**

```
org.apache.spark.sql.AnalysisException: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]
Cannot write incompatible data for the table `test`.`obkv_string_to_type_test`:
Cannot safely cast `col_tinyint` "STRING" to "INT".
```

**原因分析：**
1. Spark SQL 的 `TableOutputResolver` 在分析写入计划时会检查源列类型是否可以安全转换为目标列类型
2. 对于非字符串类型列（如 INT、BIGINT、DOUBLE 等），Spark 不允许从 STRING 类型进行隐式转换
3. 这是 Spark 的安全机制，防止数据类型不匹配导致的运行时错误

#### 6.4.2 使用声明式 Schema 方式（绕过 Spark 类型检查）

**测试方法：** 使用 `spark.createDataFrame(data, schema)` 创建 DataFrame，其中 schema 中所有列都定义为 StringType，绕过 Spark 的类型安全检查。

**测试结果：**

```
com.alipay.oceanbase.rpc.exception.ObTableException: [-10511][OB_KV_COLUMN_TYPE_NOT_MATCH]
[Column type for 'id' not match, schema column type is 'INT', input column type is 'VARCHAR']
```

**原因分析：**
1. 虽然通过声明式 Schema 绕过了 Spark 的类型检查，但 OBKV 服务端在写入时会进行严格的类型检查
2. OBKV 服务端不允许将 VARCHAR 类型写入 INT 类型的列
3. 这是 OBKV 服务端的限制，不是 Spark 或 OBKV 客户端的限制

**最终结论：**
- DECIMAL、DATE 等不支持的数据类型，无法通过字符串写入的方式绕过限制
- 即使绕过 Spark 的类型检查，OBKV 服务端仍会拒绝类型不匹配的数据
- 建议使用 JDBC 通道写入这些类型的数据，或修改表结构使用支持的类型

### 6.5 OBKV 类型支持详情（基于源码分析）

基于 obkv-table-client-java 项目源码分析，OBKV 客户端对数据类型的支持情况如下：

#### 6.4.1 完全支持的类型

| OceanBase 类型 |  ObObjType 枚举值   |  ObTableObjType 枚举值   |           说明            |
|--------------|------------------|-----------------------|-------------------------|
| NULL         | ObNullType       | ObTableNullType       | 空类型                     |
| TINYINT      | ObTinyIntType    | ObTableTinyIntType    | 8位有符号整数，也用于 BOOLEAN     |
| SMALLINT     | ObSmallIntType   | ObTableSmallIntType   | 16位有符号整数                |
| INT          | ObInt32Type      | ObTableInt32Type      | 32位有符号整数                |
| BIGINT       | ObInt64Type      | ObTableInt64Type      | 64位有符号整数                |
| VARCHAR      | ObVarcharType    | ObTableVarcharType    | 可变长度字符串                 |
| VARBINARY    | ObVarcharType    | ObTableVarbinaryType  | 可变长度二进制（CS_TYPE_BINARY） |
| FLOAT        | ObFloatType      | ObTableFloatType      | 单精度浮点数                  |
| DOUBLE       | ObDoubleType     | ObTableDoubleType     | 双精度浮点数                  |
| TIMESTAMP    | ObTimestampType  | ObTableTimestampType  | 时间戳类型                   |
| DATETIME     | ObDateTimeType   | ObTableDateTimeType   | 日期时间类型                  |
| DATE         | ObDateType       | -                     | 日期类型（读取和写入均有问题）         |
| CHAR         | ObCharType       | ObTableCharType       | 定长字符串                   |
| TEXT         | ObTextType       | ObTableTextType       | 文本类型                    |
| TINYTEXT     | ObTinyTextType   | ObTableTinyTextType   | 小文本类型                   |
| MEDIUMTEXT   | ObMediumTextType | ObTableMediumTextType | 中等文本类型                  |
| LONGTEXT     | ObLongTextType   | ObTableLongTextType   | 长文本类型                   |
| BLOB         | ObTextType       | ObTableBlobType       | 二进制大对象                  |
| TINYBLOB     | ObTinyTextType   | ObTableTinyBlobType   | 小二进制对象                  |
| MEDIUMBLOB   | ObMediumTextType | ObTableMediumBlobType | 中等二进制对象                 |
| LONGBLOB     | ObLongTextType   | ObTableLongBlobType   | 长二进制对象                  |

#### 6.4.2 不支持或有问题的类型

| OceanBase 类型 |  ObObjType 枚举值  |        状态        |                                        说明                                        |
|--------------|-----------------|------------------|----------------------------------------------------------------------------------|
| DECIMAL      | ObNumberType    | 不支持              | encode/decode 方法抛出 FeatureNotSupportedException，无法序列化和反序列化                       |
| TIME         | ObTimeType      | TODO not support | 时间类型，解码时抛出 FeatureNotSupportedException                                          |
| YEAR         | ObYearType      | TODO not support | 年份类型，解码时抛出 FeatureNotSupportedException                                          |
| BIT          | ObBitType       | TODO not support | 位类型，parseToComparable 抛出异常                                                       |
| HEX STRING   | ObHexStringType | @Deprecated      | 已废弃，encode 返回空数组                                                                 |
| UNKNOWN      | ObUnknownType   | @Deprecated      | 已废弃，用于预编译语句中的问号占位符                                                               |
| TEXT         | ObTextType      | 解码异常             | 虽然源码中定义了类型支持，但实际解码时会抛出 "payload decode meet exception"                           |
| VARBINARY    | ObVarcharType   | 解码异常             | 虽然源码中定义了类型支持，但实际解码时会抛出 "payload decode meet exception"                           |
| DATE         | ObDateType      | 读写均不支持           | OBKV 客户端将 java.sql.Date 映射为 ObDateTimeType，导致 DATE 列写入失败；读取时返回错误的日期值（1970-01-01） |

**重要说明：**

1. **DECIMAL 类型限制**：DECIMAL 类型在 obkv-table-client-java 中通过 `ObNumberType` 表示，但其 `encode` 和 `decode` 方法会抛出 `FeatureNotSupportedException`。这意味着 OBKV 协议无法序列化和反序列化 DECIMAL 类型数据。当前 Spark Connector 的解决方案是将 Decimal 转为 Double 写入，但这会导致精度丢失。建议需要精确计算的场景使用 JDBC 通道。

2. **TEXT 和 VARBINARY 类型限制**：虽然源码中定义了相应的 ObTableObjType 枚举值（ObTableTextType、ObTableVarbinaryType），但在实际使用 OBKV 协议读取包含这些类型列的表时，服务端返回的数据在解码过程中会抛出 `RuntimeException: payload decode meet exception`。这是 OBKV 协议层面的限制，建议使用 VARCHAR 替代 TEXT，避免使用 VARBINARY 类型。

3. **DATE 类型读写限制**：OBKV 客户端在 `ObObjType.valueOf(Object object)` 方法中，将 `java.sql.Date` 和 `java.util.Date` 都映射为 `ObDateTimeType`，而不是 `ObDateType`。这导致以下问题：

   - **写入失败**：当尝试向 DATE 类型列写入数据时，OBKV 服务端会报错：`Column type for 'col_date' not match, schema column type is 'MYSQL_DATE', input column type is 'DATETIME'`
   - **读取异常**：当通过 OBKV 协议读取 DATE 类型列时，返回的日期值不正确（如返回 `1970-01-01` 而不是实际存储的 `2024-03-24`），这是 OBKV 协议解码层面的问题

   建议使用 TIMESTAMP/DATETIME 类型替代 DATE，或通过 JDBC 通道读写 DATE 类型数据。

   源码证据（ObObjType.java）：

   ```java
   public static ObObjType valueOfType(Object object) {
       // ...
       } else if (object instanceof java.sql.Date) {
           return ObDateTimeType;  // java.sql.Date 被映射为 ObDateTimeType
       } else if (object instanceof Date) {
           return ObDateTimeType;  // java.util.Date 也被映射为 ObDateTimeType
       }
       // ...
   }
   ```

#### 6.4.3 类型映射实现细节

OBKV 客户端通过以下方式实现类型推断和转换：

```java
// ObObjType.java 中的 valueOfType 方法
public static ObObjType valueOfType(Object object) {
    if (object == null) return ObNullType;
    else if (object instanceof Boolean) return ObTinyIntType;
    else if (object instanceof Byte) return ObTinyIntType;
    else if (object instanceof Short) return ObSmallIntType;
    else if (object instanceof Integer) return ObInt32Type;
    else if (object instanceof Long) return ObInt64Type;
    else if (object instanceof String) return ObVarcharType;
    else if (object instanceof byte[]) return ObVarcharType;
    else if (object instanceof Float) return ObFloatType;
    else if (object instanceof Double) return ObDoubleType;
    else if (object instanceof Date) return ObDateTimeType;
    else if (object instanceof Timestamp) return ObTimestampType;
    // ...
}
```

#### 6.4.4 Spark Connector 类型转换

Spark Connector 通过 `OBKVTypeConverter` 实现类型转换：

**读取时（OBKV → Spark）：**
- BooleanType: 直接映射或通过数值转换
- ByteType/ShortType/IntegerType/LongType: 通过 Number 接口转换
- FloatType/DoubleType: 直接映射
- StringType: 通过 toString() 转换
- DecimalType: 转为 BigDecimal 后创建 Decimal
- DateType: 支持 java.sql.Date, LocalDate, java.util.Date
- TimestampType: 支持 java.sql.Timestamp, LocalDateTime
- BinaryType: 直接映射 byte[]

**写入时（Spark → OBKV）：**
- BooleanType → java.lang.Boolean
- ByteType → java.lang.Byte
- ShortType → java.lang.Short
- IntegerType → java.lang.Integer
- LongType → java.lang.Long
- FloatType → java.lang.Float
- DoubleType → java.lang.Double
- StringType → java.lang.String
- DecimalType → java.lang.Double（精度丢失）
- DateType → java.sql.Date
- TimestampType → java.sql.Timestamp
- BinaryType → byte[]

## 七、测试覆盖

### 7.1 单元测试

- OBKVTypeConverterTest: 类型转换测试（26 个测试用例）
- OBKVFilterCompilerTest: 过滤器编译测试

### 7.2 集成测试

|  测试类别  |                    测试内容                    |
|--------|--------------------------------------------|
| 基础读写   | Catalog 写入读取、DataFrame 写入读取、JDBC 验证        |
| 类型测试   | 全类型读取、全类型写入、边界值测试、Decimal 精度测试             |
| 功能测试   | 谓词下推、列裁剪、复合主键、批量刷新、insertOrUpdate、Null 值处理 |
| 分区测试   | 分区表读写、分区表谓词下推                              |
| 大数据量测试 | 大批量写入（100 行）、批次刷新测试                        |

## 七、最佳实践

|    场景     |        推荐通道        |
|-----------|--------------------|
| 通用读写      | JDBC               |
| 大批量导入     | Direct Load        |
| 高频小批量写入   | OBKV               |
| Oracle 租户 | JDBC 或 Direct Load |

---

文档版本：1.0
最后更新：2026-03-24
