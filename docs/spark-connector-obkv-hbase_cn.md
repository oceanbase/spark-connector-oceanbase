## Spark Connector OBKV HBase

[English](spark-connector-obkv-hbase.md) | 简体中文

本项目是一个 OBKV HBase 的 Spark Connector，可以在 Spark 中通过 [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java) 将数据写入到 OceanBase。

> **注意**：当前版本仅支持**写入操作**，不支持从 HBase 读取数据。

## 版本兼容

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Connector</th>
                <th class="text-left" style="width: 10%">Spark</th>
                <th class="text-left" style="width: 15%">OceanBase</th>
                <th class="text-left" style="width: 10%">Java</th>
                <th class="text-left" style="width: 10%">Scala</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>1.0</td>
                <td style="word-wrap: break-word;">2.4, 3.1 ~ 3.4</td>
                <td>4.2.x及以后的版本</td>
                <td>8</td>
                <td>2.12</td>
            </tr>
        </tbody>
    </table>
</div>

- 注意：如果需要基于其他 scala 版本构建的程序包, 您可以通过源码构建的方式获得程序包

## 获取程序包

您可以在 [Releases 页面](https://github.com/oceanbase/spark-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/spark-connector-obkv-hbase) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-obkv-hbase-3.4_2.12</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-obkv-hbase-3.4_2.12</artifactId>
    <version>${project.version}</version>
</dependency>

<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <name>Sonatype Snapshot Repository</name>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

当然您也可以通过源码构建的方式获得程序包。
- 默认以scala 2.12版本进行构建
- 编译成功后，会在各个版本对应的模块下的target目录生成目标 jar 包，如：spark-connector-obkv-hbase-3.4_2.12-1.0-SNAPSHOT.jar。 将此文件复制到 Spark 的 ClassPath 中即可使用 spark-connector-obkv-hbase。

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -DskipTests
```

- 如果需要其他 scala 版本，请参考下面以 scala 2.11版本构建命令

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.11.12 -Dscala.binary.version=2.11 -DskipTests
```

## 使用示例

以从Hive同步数据到OceanBase为例

### 准备工作

创建对应的Hive表和OceanBase表，为数据同步做准备

- 通过${SPARK_HOME}/bin/spark-sql命令，开启spark-sql

```sql
CREATE TABLE test.orders (
  order_id     INT,
  order_date   TIMESTAMP,
  customer_name string,
  price        double,
  product_id   INT,
  order_status BOOLEAN
) using parquet;

insert into orders values
(1, now(), 'zs', 12.2, 12, true),
(2, now(), 'ls', 121.2, 12, true),
(3, now(), 'xx', 123.2, 12, true),
(4, now(), 'jac', 124.2, 12, false),
(5, now(), 'dot', 111.25, 12, true);
```

- 连接到 OceanBase 并创建 HBase 表。在 OceanBase HBase 模式下，每个列族是一个独立的物理表，命名格式为 `表名$列族名`：

```sql
use test;

-- 创建 family1 列族对应的表
CREATE TABLE `htable$family1`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);

-- 创建 family2 列族对应的表（如果需要多列族）
CREATE TABLE `htable$family2`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);
```

### Schema 定义

连接器使用 Spark 的 STRUCT 类型来定义 Spark 和 HBase 之间的 schema 映射：

- **第一个字段**必须是 rowkey 列
- **后续字段**代表列族（column family），每个字段定义为 STRUCT 类型
- **STRUCT 内部的字段**代表该列族内的列（字段名即为列限定符 qualifier）

单列族 schema 示例：

```
rowkey STRING,
family1 STRUCT<col1: STRING, col2: INT>
```

多列族 schema 示例：

```
rowkey STRING,
family1 STRUCT<col1: STRING, col2: INT>,
family2 STRUCT<col3: DOUBLE, col4: BOOLEAN>
```

### Config Url 模式

#### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_obkv (
  rowkey STRING,
  family1 STRUCT<
    order_date: TIMESTAMP,
    customer_name: STRING,
    price: DOUBLE,
    product_id: INT,
    order_status: BOOLEAN
  >
)
USING `obkv-hbase`
OPTIONS(
  "url" = "http://localhost:8080/services?Action=ObRootServiceInfo&ObRegion=myob",
  "sys.username"= "root",
  "sys.password" = "password",
  "schema-name"="test",
  "table-name"="htable",
  "username"="root@sys#myob",
  "password"="password"
);

INSERT INTO test_obkv
SELECT
  CAST(order_id AS STRING) as rowkey,
  STRUCT(order_date, customer_name, price, product_id, order_status) as family1
FROM test.orders;
```

#### DataFrame API

```scala
import org.apache.spark.sql.functions._

val sourceDf = spark.sql("select * from test.orders")

// 转换为 STRUCT schema 并写入
sourceDf
  .select(
    col("order_id").cast("string").as("rowkey"),
    struct("order_date", "customer_name", "price", "product_id", "order_status").as("family1")
  )
  .write
  .format("obkv-hbase")
  .option("url", "http://localhost:8080/services?Action=ObRootServiceInfo&ObRegion=myob")
  .option("sys.username", "root")
  .option("sys.password", "password")
  .option("username", "root@sys#myob")
  .option("password", "password")
  .option("schema-name", "test")
  .option("table-name", "htable")
  .save()
```

### ODP 模式

#### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_obkv (
  rowkey STRING,
  family1 STRUCT<
    order_date: TIMESTAMP,
    customer_name: STRING,
    price: DOUBLE,
    product_id: INT,
    order_status: BOOLEAN
  >
)
USING `obkv-hbase`
OPTIONS(
  "odp-mode" = true,
  "odp-ip"= "localhost",
  "odp-port" = "2885",
  "schema-name"="test",
  "table-name"="htable",
  "username"="root@sys#myob",
  "password"="password"
);

INSERT INTO test_obkv
SELECT
  CAST(order_id AS STRING) as rowkey,
  STRUCT(order_date, customer_name, price, product_id, order_status) as family1
FROM test.orders;
```

#### DataFrame API

```scala
import org.apache.spark.sql.functions._

val sourceDf = spark.sql("select * from test.orders")

// 转换为 STRUCT schema 并写入
sourceDf
  .select(
    col("order_id").cast("string").as("rowkey"),
    struct("order_date", "customer_name", "price", "product_id", "order_status").as("family1")
  )
  .write
  .format("obkv-hbase")
  .option("odp-mode", true)
  .option("odp-ip", "localhost")
  .option("odp-port", 2885)
  .option("username", "root@sys#myob")
  .option("password", "password")
  .option("schema-name", "test")
  .option("table-name", "htable")
  .save()
```

### 多列族支持

您可以通过定义多个 STRUCT 字段来写入多个列族。注意需要先在 OceanBase 中创建对应的表（如 `htable$family1` 和 `htable$family2`）：

```sql
CREATE TEMPORARY VIEW test_obkv (
  rowkey STRING,
  family1 STRUCT<col1: STRING, col2: INT>,
  family2 STRUCT<col3: DOUBLE, col4: BOOLEAN>
)
USING `obkv-hbase`
OPTIONS(
  "odp-mode" = true,
  "odp-ip"= "localhost",
  "odp-port" = "2885",
  "schema-name"="test",
  "table-name"="htable",
  "username"="root@sys#myob",
  "password"="password"
);

INSERT INTO test_obkv
SELECT
  'rowkey_value' as rowkey,
  named_struct('col1', 'value1', 'col2', 123) as family1,
  named_struct('col3', 456.78, 'col4', true) as family2;
```

## 配置项

<table>
  <thead>
    <tr>
      <th>参数名</th>
      <th>是否必需</th>
      <th>默认值</th>
      <th>类型</th>
      <th>描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>schema-name</td>
      <td>是</td>
      <td></td>
      <td>String</td>
      <td>OceanBase 的 db 名。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>是</td>
      <td></td>
      <td>String</td>
      <td>HBase 表名（不带 <code>$family</code> 后缀）。OceanBase HBase 表的命名格式为 <code>表名$列族名</code>，但此处只需指定基础表名。连接器会根据 schema 中的 STRUCT 字段名自动将数据路由到正确的列族表。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>是</td>
      <td></td>
      <td>String</td>
      <td>非 sys 租户的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>是</td>
      <td></td>
      <td>String</td>
      <td>非 sys 租户的密码。</td>
    </tr>
    <tr>
      <td>odp-mode</td>
      <td>否</td>
      <td>false</td>
      <td>Boolean</td>
      <td>如果设置为 'true'，连接器将通过 ODP 连接到 OBKV，否则通过 config url 连接。</td>
    </tr>
    <tr>
      <td>url</td>
      <td>否</td>
      <td></td>
      <td>String</td>
      <td>集群的 config url，可以通过 <code>SHOW PARAMETERS LIKE 'obconfig_url'</code> 查询。当 'odp-mode' 为 'false' 时必填。</td>
    </tr>
    <tr>
      <td>sys.username</td>
      <td>否</td>
      <td></td>
      <td>String</td>
      <td>sys 租户的用户名，当 'odp-mode' 为 'false' 时必填。</td>
    </tr>
    <tr>
      <td>sys.password</td>
      <td>否</td>
      <td></td>
      <td>String</td>
      <td>sys 租户用户的密码，当 'odp-mode' 为 'false' 时必填。</td>
    </tr>
    <tr>
      <td>odp-ip</td>
      <td>否</td>
      <td></td>
      <td>String</td>
      <td>ODP 的 IP，当 'odp-mode' 为 'true' 时必填。</td>
    </tr>
    <tr>
      <td>odp-port</td>
      <td>否</td>
      <td>2885</td>
      <td>Integer</td>
      <td>ODP 的 RPC 端口，当 'odp-mode' 为 'true' 时必填。</td>
    </tr>
    <tr>
      <td>hbase.properties</td>
      <td>否</td>
      <td></td>
      <td>String</td>
      <td>配置 'obkv-hbase-client-java' 的属性，多个值用分号分隔。</td>
    </tr>
    <tr>
      <td>batch-size</td>
      <td>否</td>
      <td>10000</td>
      <td>Integer</td>
      <td>一次写入OceanBase的批大小。</td>
    </tr>
  </tbody>
</table>

