## Spark Connector OBKV HBase

English | [简体中文](spark-connector-obkv-hbase_cn.md)

This is the spark connector for OBKV HBase mode, which can be used to write data to OceanBase via [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java).

## Version compatibility

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
                <td>4.2.x or later versions</td>
                <td>8</td>
                <td>2.12</td>
            </tr>
        </tbody>
    </table>
</div>

- Note: If you need a package built based on other Scala versions, you can get the package by building it from source code.

## Get the package

You can get the release packages at [Releases Page](https://github.com/oceanbase/spark-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/spark-connector-obkv-hbase).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-obkv-hbase-3.4_2.12</artifactId>
    <version>${project.version}</version>
</dependency>
```

If you'd rather use the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

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

Of course, you can also get the package by building from source code.

- By default, it is built with scala version 2.12
- After successful compilation, the target jar package will be generated in the target directory under the module corresponding to each version, such as: spark-connector-obkv-hbase-3.4_2.12-1.0-SNAPSHOT.jar. Copy this file to Spark's ClassPath to use spark-connector-obkv-hbase.

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -DskipTests
```

- If you need a package built based on other Scala versions, refer to the command below to build based on Scala 2.11.

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.11.12 -Dscala.binary.version=2.11 -DskipTests
```

## Usage Examples

Take synchronizing data from Hive to OceanBase as an example.

### Preparation

Create corresponding Hive tables and OceanBase tables to prepare for data synchronization

- Start spark-sql by running `${SPARK_HOME}/bin/spark-sql`

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

- Connect to OceanBase and create HBase tables. In OceanBase HBase mode, each column family is a separate physical table with the naming convention `table_name$family_name`:

```sql
use test;

-- Create table for family1 column family
CREATE TABLE `htable$family1`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);

-- Create table for family2 column family (if you need multiple column families)
CREATE TABLE `htable$family2`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);
```

### Schema Definition

The connector uses Spark's STRUCT types to define the schema mapping between Spark and HBase:

- The **first field** must be the rowkey column (typically a STRING type)
- **Subsequent fields** represent column families, each defined as a STRUCT type
- **Fields inside STRUCT** represent columns within that column family (the field name is the column qualifier)

Example schema with single column family:

```
rowkey STRING,
family1 STRUCT<col1: STRING, col2: INT>
```

Example schema with multiple column families:

```
rowkey STRING,
family1 STRUCT<col1: STRING, col2: INT>,
family2 STRUCT<col3: DOUBLE, col4: BOOLEAN>
```

### Config Url Mode

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

// Transform to STRUCT schema and write
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

### ODP Mode

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

// Transform to STRUCT schema and write
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

### Multiple Column Families

You can write to multiple column families by defining multiple STRUCT fields. Note that you need to create corresponding tables in OceanBase first (e.g., `htable$family1` and `htable$family2`):

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

## Configuration

<table>
 <thead>
   <tr>
     <th>Option</th>
     <th>Required</th>
     <th>Default</th>
     <th>Type</th>
     <th>Description</th>
   </tr>
 </thead>
 <tbody>
   <tr>
     <td>schema-name</td>
     <td>Yes</td>
     <td></td>
     <td>String</td>
     <td>The database name of OceanBase.</td>
   </tr>
   <tr>
     <td>table-name</td>
     <td>Yes</td>
     <td></td>
     <td>String</td>
     <td>The HBase table name (without <code>$family</code> suffix). OceanBase HBase tables are named as <code>table_name$family_name</code>, but you only need to specify the base table name here. The connector automatically routes data to the correct family table based on the STRUCT field names in your schema.</td>
   </tr>
   <tr>
     <td>username</td>
     <td>Yes</td>
     <td></td>
     <td>String</td>
     <td>The username of non-sys tenant user.</td>
   </tr>
   <tr>
     <td>password</td>
     <td>Yes</td>
     <td></td>
     <td>String</td>
     <td>The password of non-sys tenant user.</td>
   </tr>
   <tr>
     <td>odp-mode</td>
     <td>No</td>
     <td>false</td>
     <td>Boolean</td>
     <td>If set to 'true', the connector will connect to OBKV via ODP, otherwise via config url.</td>
   </tr>
   <tr>
     <td>url</td>
     <td>No</td>
     <td></td>
     <td>String</td>
     <td>The config url, can be queried by <code>SHOW PARAMETERS LIKE 'obconfig_url'</code>. Required when 'odp-mode' is set to 'false'.</td>
   </tr>
   <tr>
     <td>sys.username</td>
     <td>No</td>
     <td></td>
     <td>String</td>
     <td>The username of sys tenant. Required if 'odp-mode' is set to 'false'.</td>
   </tr>
   <tr>
     <td>sys.password</td>
     <td>No</td>
     <td></td>
     <td>String</td>
     <td>The password of sys tenant. Required if 'odp-mode' is set to 'false'.</td>
   </tr>
   <tr>
     <td>odp-ip</td>
     <td>No</td>
     <td></td>
     <td>String</td>
     <td>IP address of the ODP. Required if 'odp-mode' is set to 'true'.</td>
   </tr>
   <tr>
     <td>odp-port</td>
     <td>No</td>
     <td>2885</td>
     <td>Integer</td>
     <td>RPC port of ODP. Required if 'odp-mode' is set to 'true'.</td>
   </tr>
   <tr>
     <td>hbase.properties</td>
     <td>No</td>
     <td></td>
     <td>String</td>
     <td>Properties to configure 'obkv-hbase-client-java', multiple values are separated by semicolons.</td>
   </tr>
   <tr>
     <td>batch-size</td>
     <td>No</td>
     <td>10000</td>
     <td>Integer</td>
     <td>The size of the batch that is written to the OceanBase at one time.</td>
   </tr>
 </tbody>
</table>

