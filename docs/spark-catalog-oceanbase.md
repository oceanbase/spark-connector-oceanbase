# OceanBase Spark Catalog

English | [简体中文](spark-catalog-oceanbase_cn.md)

OceanBase Spark Connector fully supports Spark Catalog since version 1.1, which provides a more convenient and efficient solution for users to use OceanBase in Spark.
By using Spark Catalog, users can access and operate OceanBase databases in a more concise and consistent way.

## Currently supported features

- Currently only supports OceanBase MySQL mode.
- Supports Spark adaptive partitioning and parallel reading of OceanBase (via JDBC).
  - Predicate pushdown support.
- Supports writing OceanBase through direct load.
- Supports writing OceanBase through JDBC.
  - For primary key tables, supports writing in upsert mode.
    - MySQL mode is based on: `INSERT INTO ... ON DUPLICATE KEY UPDATE` syntax.
  - For non-primary key tables, write through `INSERT INTO`.
- Supports managing databases and tables in OceanBase through Spark-SQL, including: show databases, show tables, drop table, drop database and other syntax support.
  - Supports CTAS syntax to create and write OceanBase tables.

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
                <td>1.1 or later versions</td>
                <td style="word-wrap: break-word;">3.1 ~ 3.5</td>
                <td>
                  <ul>
                    <li>JDBC: 3.x, 4.x</li>
                    <li>Direct Load: 4.2.x or later versions</li>
                  </ul>
                </td>
                <td>8</td>
                <td>2.12</td>
            </tr>
        </tbody>
    </table>
</div>

- Note: If you need a package built based on other Scala versions, you can get the package by building it from source code.

## Get the package

You can get the release packages at [Releases Page](https://github.com/oceanbase/spark-connector-oceanbase/releases) or [Maven Central](https://central.sonatype.com/artifact/com.oceanbase/spark-connector-oceanbase).

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-oceanbase-3.4_2.12</artifactId>
    <version>${project.version}</version>
</dependency>
```

If you'd rather use the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-oceanbase-3.4_2.12</artifactId>
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
- After successful compilation, the target jar package will be generated in the target directory under the module corresponding to each version, such as: spark-connector-oceanbase-3.4_2.12-1.0-SNAPSHOT.jar. Copy this file to Spark's ClassPath to use spark-connector-oceanbase.

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -DskipTests
```

- If you need a package built based on other Scala versions, refer to the command below to build based on Scala 2.13.

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.13.15 -Dscala.binary.version=2.13 -DskipTests
```

## How to configure

Set OceanBase Catalog related parameters when starting Spark-SQL CLI

```shell
./bin/spark-sql \
--conf "spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog" \
--conf "spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881" \
--conf "spark.sql.catalog.your_catalog_name.username=root@test" \
--conf "spark.sql.catalog.your_catalog_name.password=******" \
--conf "spark.sql.catalog.your_catalog_name.schema-name=test" \
--conf "spark.sql.defaultCatalog=your_catalog_name"
```

Of course, you can also configure the relevant parameters in the spark configuration file, the default is spark-defaults.conf, as shown below, and then directly start the Spark-SQL CLI.

```shell
spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog
spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881
spark.sql.catalog.your_catalog_name.username=root@test
spark.sql.catalog.your_catalog_name.password=******
spark.sql.catalog.your_catalog_name.schema-name=test
spark.sql.defaultCatalog=your_catalog_name
```

For DataFrame API, you can configure to use OceanBase Catalog in the following way:

```scala
val spark = SparkSession
  .builder()
  .master("local[*]")
  .config("spark.sql.catalog.your_catalog_name", "com.oceanbase.spark.catalog.OceanBaseCatalog")
  .config("spark.sql.catalog.your_catalog_name.url", "jdbc:mysql://localhost:2881")
  .config("spark.sql.catalog.your_catalog_name.username", "root@test")
  .config("spark.sql.catalog.your_catalog_name.password", "******")
  .config("spark.sql.catalog.your_catalog_name.schema-name", "test")
  .config("spark.sql.defaultCatalog", "your_catalog_name")
  .getOrCreate()
```

## Usage examples

### Manage databases and tables in OceanBase through Spark-SQL

```sql
-- show all databases
show databases;

-- drop database test
drop database test;

-- create database test
create database test;

-- use and switch databases
use your_oceanbase_db;

-- show tables in test
show tables;

-- create table test1
CREATE TABLE test.test1(
  user_id BIGINT COMMENT 'test_for_key',
  name VARCHAR(255)
)
PARTITIONED BY (bucket(16, user_id))
COMMENT 'test_for_table_create'
TBLPROPERTIES('replica_num' = 2, COMPRESSION = 'zstd_1.0');

-- drop table test1
drop table test.test1;

```

### Read

```sql
SELECT * FROM test.test;
```

### Write

Take synchronizing data from Hive to OceanBase as an example

#### Preparation

Create corresponding Hive tables and OceanBase tables to prepare for data synchronization

- Start spark-sql by running `${SPARK_HOME}/bin/spark-sql`

```sql
CREATE TABLE spark_catalog.default.orders (
  order_id     INT,
  order_date   TIMESTAMP,
  customer_name string,
  price        double,
  product_id   INT,
  order_status BOOLEAN
) using parquet;

insert into spark_catalog.default.orders values
(1, now(), 'zs', 12.2, 12, true),
(2, now(), 'ls', 121.2, 12, true),
(3, now(), 'xx', 123.2, 12, true),
(4, now(), 'jac', 124.2, 12, false),
(5, now(), 'dot', 111.25, 12, true);
```

- Connect to OceanBase

```sql
CREATE TABLE test.orders (
  order_id     INT PRIMARY KEY,
  order_date   TIMESTAMP,
  customer_name VARCHAR(225),
  price        double,
  product_id   INT,
  order_status BOOLEAN
);
```

#### Via JDBC

```sql
insert into table test.orders
select * from spark_catalog.default.orders;
```

#### Via Direct-Load

Add the following direct-load related parameters to the spark configuration file, which defaults to spark-defaults.conf, as shown below, and then restart the Spark-SQL CLI.

```shell
spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog
spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881
spark.sql.catalog.your_catalog_name.username=root@test
spark.sql.catalog.your_catalog_name.password=******
spark.sql.catalog.your_catalog_name.schema-name=test
spark.sql.defaultCatalog=your_catalog_name

# enable direct-load
spark.sql.catalog.your_catalog_name.direct-load.enabled=true;
spark.sql.catalog.your_catalog_name.direct-load.host=localhost;
spark.sql.catalog.your_catalog_name.direct-load.rpc-port=2882;
```

```sql
insert into table test.orders
select * from spark_catalog.default.orders;
```

Precautions for direct-load:

- Table locking will occur during the direct-load job. While the table is locked:
  - Data write operations and DDL changes are prohibited.
  - Data queries are allowed.

## Configuration

### General configuration

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 10%">Required</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>spark.sql.catalog.your_catalog_name</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Sets the class name of the Catalog provider. For OceanBase, the only valid value is com.oceanbase.spark.catalog.OceanBaseCatalog.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.url</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The JDBC url to connect to OceanBase.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.username</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The username.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.password</td>
                <td>Yes</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The password.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.driver</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The class name of the JDBC driver to use to connect to this URL.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.schema-name</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Set the default schema for the OceanBase Catalog.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.fetch-size</td>
                <td>No</td>
                <td style="word-wrap: break-word;">100</td>
                <td>Int</td>
                <td>The JDBC fetch size, which determines how many rows to fetch per round trip.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.batch-size</td>
                <td>No</td>
                <td style="word-wrap: break-word;">1024</td>
                <td>Int</td>
                <td>The JDBC writing batch size, which determines how many rows to insert per round trip.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.max-records-per-partition</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>Int</td>
                <td>Controls the maximum number of data that can be used as a Spark partition when Spark reads OBs. The default is empty, and Spark will automatically calculate a reasonable value based on the amount of data. Note: It is generally not recommended to set this parameter.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.parallel-hint-degree</td>
                <td>No</td>
                <td style="word-wrap: break-word;">1</td>
                <td>Int</td>
                <td>The SQL statements sent by Spark to OB will automatically carry PARALLEL Hint. This parameter can be used to adjust the parallelism, and the default value is 1.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.statistics-parallel-hint-degree</td>
                <td>No</td>
                <td style="word-wrap: break-word;">4</td>
                <td>Int</td>
                <td>Controls the parallelism level for statistical queries (e.g., COUNT, MIN, MAX) by adding /*+ PARALLEL(N) */ hint to generated SQL.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.pushdown-query-hints</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Specifies hints to be pushed down to OceanBase SELECT statements. These hints will be added to the final SQL sent to OceanBase for execution. Supports multiple hints (separated by spaces), e.g., 'READ_CONSISTENCY(WEAK) query_timeout(10000000)'.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.pushdown-write-hints</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Specifies hints to be pushed down to OceanBase INSERT statements. These hints will be added to the final SQL sent to OceanBase for execution. Supports multiple hints (separated by spaces), e.g., 'append parallel(16)'.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.partition-compute-parallelism</td>
                <td>No</td>
                <td style="word-wrap: break-word;">32</td>
                <td>Int</td>
                <td>Controls the parallelism level for partition computation. This parameter determines the number of threads used when computing partitions for partitioned tables (mainly through parallel SQL queries to OceanBase partition statistics). The computation task runs on the driver node. Higher values can improve performance for tables with many partitions. When setting a larger value for this parameter, appropriately increasing the CPU cores and memory of the driver node can achieve better performance.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.partition-compute-timeout-minutes</td>
                <td>No</td>
                <td style="word-wrap: break-word;">10</td>
                <td>Int</td>
                <td>Timeout in minutes for partition computation. This parameter controls how long to wait for partition computation to complete before throwing a timeout exception.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.query-timeout-hint-degree</td>
                <td>No</td>
                <td style="word-wrap: break-word;">-1</td>
                <td>Int</td>
                <td>Control the query timeout by adding /*+ query_timeout(N) */ hint to the generated SQL. This parameter can be used to specify the timeout in microseconds. The default value is -1, which means that the hint is not added.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.disable-pk-table-use-where-partition</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When true, primary key tables will be prohibited from using WHERE clause partitioning.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.{database}.{table}.partition-column</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>You can manually specify the primary key table partition column, and by default, one will be automatically selected from the primary key columns.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.enable-autocommit</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When using jdbc to write, whether to enable autocommit for automatic transaction commit.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.use-insert-ignore</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When enabled, uses INSERT IGNORE instead of INSERT ... ON DUPLICATE KEY UPDATE for handling primary key conflicts. INSERT IGNORE will skip rows with duplicate keys and continue processing, while ON DUPLICATE KEY UPDATE will update existing rows with new values.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.upsert-by-unique-key</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When a table has both primary key and unique key index, this option controls which key to use for determining conflict detection. If set to true, uses unique key for conflict detection and updates all columns except unique key columns (including primary key columns). If set to false (default), uses primary key for conflict detection and updates all columns except primary key columns.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.optimize-decimal-string-comparison</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When this option is true, DECIMAL(P, 0) columns with precision <= 19 will be converted to BIGINT (LongType) to avoid precision loss when comparing with string literals. This optimization prevents Spark from converting String + DECIMAL to DOUBLE (which loses precision for large numbers). If false (default), DECIMAL columns will remain as DecimalType. Note: This optimization only applies to integer DECIMAL with scale = 0 and precision <= 19 (BIGINT range: -9223372036854775808 to 9223372036854775807).</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.string-as-varchar-length</td>
                <td>No</td>
                <td style="word-wrap: break-word;">1024</td>
                <td>Int</td>
                <td>Defines the length of VARCHAR type when mapping String types during table creation. Default: 1024.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-string-as-text</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When this option is true, the string type of spark will be converted to text type of OceanBase when creating a table.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-spark-varchar-datatype</td>
                <td>No</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>When this option is true, the varchar type of OceanBase will be converted to spark's varchar type. Note that spark varchar type is an experimental feature.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-always-nullable</td>
                <td>No</td>
                <td style="word-wrap: break-word;">true</td>
                <td>Boolean</td>
                <td>Forces all fields to be marked as nullable during schema inference, regardless of the database metadata's nullability constraints. This provides a safety net for handling data sources with incomplete metadata or implicit null values.</td>
            </tr>
            <tr>
                <td>spark.sql.defaultCatalog</td>
                <td>No</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>Set the Spark SQL default catalog.</td>
            </tr>
        </tbody>
    </table>
</div>

### Direct load configuration

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 10%">Required</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.enabled</td>
                <td>No</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Enable direct-load writing.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.host</td>
                <td>No</td>
                <td></td>
                <td>String</td>
                <td>Hostname used in direct-load.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.rpc-port</td>
                <td>No</td>
                <td>2882</td>
                <td>Integer</td>
                <td>Rpc port used in direct-load.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.username</td>
                <td>No</td>
                <td></td>
                <td>String</td>
                <td>The direct-load's username. If this configuration is not specified, the jdbc username is used.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.odp-mode</td>
                <td>No</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Whether to use ODP proxy for direct-load. When set to true, it will connect through ODP proxy (typically port 2885) and pass the full username format (e.g., user@tenant#cluster); when set to false (default), it will connect directly to OBServer (typically port 2882).</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.parallel</td>
                <td>No</td>
                <td>8</td>
                <td>Integer</td>
                <td>The parallel of the direct-load server. This parameter determines how much CPU resources the server uses to process this import task.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.batch-size</td>
                <td>No</td>
                <td>10240</td>
                <td>Integer</td>
                <td>The size of the batch that is written to the OceanBase at one time.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.max-error-rows</td>
                <td>No</td>
                <td>0</td>
                <td>Long</td>
                <td>Maximum tolerable number of error rows.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.timeout</td>
                <td>No</td>
                <td>7d</td>
                <td>Duration</td>
                <td>The timeout for direct-load task.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.heartbeat-timeout</td>
                <td>No</td>
                <td>60s</td>
                <td>Duration</td>
                <td>Client heartbeat timeout in direct-load task.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.heartbeat-interval</td>
                <td>No</td>
                <td>10s</td>
                <td>Duration</td>
                <td>Client heartbeat interval in direct-load task.</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.load-method</td>
                <td>No</td>
                <td>full</td>
                <td>String</td>
                <td>The direct-load load mode: <code>full</code>, <code>inc</code>, <code>inc_replace</code>.
                <ul>
                    <li><code>full</code>: full direct-load, default value.</li>
                    <li><code>inc</code>: normal incremental direct-load, primary key conflict check will be performed, observer-4.3.2 and above support, direct-load.dup-action REPLACE is not supported for the time being.</li>
                    <li><code>inc_replace</code>: special replace mode incremental direct-load, no primary key conflict check will be performed, directly overwrite the old data (equivalent to the effect of replace), direct-load.dup-action parameter will be ignored, observer-4.3.2 and above support.</li>
                </ul>
                </td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.dup-action</td>
                <td>No</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>Action when there is duplicated record of direct-load task. Can be <code>STOP_ON_DUP</code>, <code>REPLACE</code> or <code>IGNORE</code>.</td>
            </tr>
        </tbody>
    </table>
</div>

## Data Type Mapping

This section describes the mapping between OceanBase data types and Spark SQL data types when reading from and writing to OceanBase.

### MySQL Mode

#### Reading Data (OceanBase → Spark)

Type mapping when reading data from OceanBase MySQL mode:

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">OceanBase Type</th>
                <th class="text-left" style="width: 30%">Spark SQL Type</th>
                <th class="text-left" style="width: 40%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td colspan="3"><b>Basic Data Types</b></td>
            </tr>
            <tr>
                <td>BIT</td>
                <td>LongType</td>
                <td>When size != 1</td>
            </tr>
            <tr>
                <td>BIT</td>
                <td>BooleanType</td>
                <td>When size = 1</td>
            </tr>
            <tr>
                <td>TINYINT</td>
                <td>BooleanType</td>
                <td></td>
            </tr>
            <tr>
                <td>SMALLINT</td>
                <td>ShortType</td>
                <td></td>
            </tr>
            <tr>
                <td>INT / INTEGER</td>
                <td>IntegerType</td>
                <td></td>
            </tr>
            <tr>
                <td>BIGINT</td>
                <td>LongType</td>
                <td></td>
            </tr>
            <tr>
                <td>FLOAT</td>
                <td>FloatType</td>
                <td></td>
            </tr>
            <tr>
                <td>DOUBLE</td>
                <td>DoubleType</td>
                <td></td>
            </tr>
            <tr>
                <td>DECIMAL(p,s)</td>
                <td>DecimalType(p,s)</td>
                <td></td>
            </tr>
            <tr>
                <td>CHAR(n)</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>VARCHAR(n)</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>TEXT</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>BINARY</td>
                <td>BinaryType</td>
                <td></td>
            </tr>
            <tr>
                <td>DATE</td>
                <td>DateType</td>
                <td></td>
            </tr>
            <tr>
                <td>DATETIME / TIMESTAMP</td>
                <td>TimestampType</td>
                <td></td>
            </tr>
            <tr>
                <td colspan="3"><b>Complex Data Types</b></td>
            </tr>
            <tr>
                <td>ARRAY(type)</td>
                <td>ArrayType(corresponding type)</td>
                <td>Supports up to 6 levels of nesting. Supported element types: INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING, etc.</td>
            </tr>
            <tr>
                <td>VECTOR(n)</td>
                <td>ArrayType(FloatType)</td>
                <td>n represents the vector dimension</td>
            </tr>
            <tr>
                <td>MAP(keyType, valueType)</td>
                <td>MapType(keyType, valueType)</td>
                <td></td>
            </tr>
            <tr>
                <td>JSON</td>
                <td>StringType</td>
                <td>JSON data is read as string</td>
            </tr>
            <tr>
                <td>ENUM(...)</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>SET(...)</td>
                <td>StringType</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>

#### Writing Data (Spark → OceanBase)

Type mapping when writing data to existing OceanBase MySQL mode tables:

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">Spark SQL Type</th>
                <th class="text-left" style="width: 30%">OceanBase Type</th>
                <th class="text-left" style="width: 40%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td colspan="3"><b>Basic Data Types</b></td>
            </tr>
            <tr>
                <td>BooleanType</td>
                <td>BOOLEAN / TINYINT</td>
                <td></td>
            </tr>
            <tr>
                <td>ByteType</td>
                <td>TINYINT</td>
                <td></td>
            </tr>
            <tr>
                <td>ShortType</td>
                <td>SMALLINT</td>
                <td></td>
            </tr>
            <tr>
                <td>IntegerType</td>
                <td>INT</td>
                <td></td>
            </tr>
            <tr>
                <td>LongType</td>
                <td>BIGINT</td>
                <td></td>
            </tr>
            <tr>
                <td>FloatType</td>
                <td>FLOAT</td>
                <td></td>
            </tr>
            <tr>
                <td>DoubleType</td>
                <td>DOUBLE</td>
                <td></td>
            </tr>
            <tr>
                <td>DecimalType(p,s)</td>
                <td>DECIMAL(p,s)</td>
                <td></td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>CHAR / VARCHAR / TEXT</td>
                <td>Depends on the actual target column type</td>
            </tr>
            <tr>
                <td>BinaryType</td>
                <td>BINARY</td>
                <td></td>
            </tr>
            <tr>
                <td>DateType</td>
                <td>DATE</td>
                <td></td>
            </tr>
            <tr>
                <td>TimestampType</td>
                <td>DATETIME / TIMESTAMP</td>
                <td></td>
            </tr>
            <tr>
                <td colspan="3"><b>Complex Data Types</b></td>
            </tr>
            <tr>
                <td>ArrayType(IntegerType)</td>
                <td>INT[] / ARRAY(INT)</td>
                <td>Table must be pre-created with ARRAY type columns</td>
            </tr>
            <tr>
                <td>ArrayType(FloatType)</td>
                <td>FLOAT[] / VECTOR(n)</td>
                <td>Can write to VECTOR or FLOAT ARRAY columns</td>
            </tr>
            <tr>
                <td>MapType(keyType, valueType)</td>
                <td>MAP(keyType, valueType)</td>
                <td>Table must be pre-created with MAP type columns</td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>JSON</td>
                <td>JSON data is written as string</td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>ENUM / SET</td>
                <td>Written string values must conform to ENUM/SET definition</td>
            </tr>
        </tbody>
    </table>
</div>

### Oracle Mode

#### Reading Data (OceanBase → Spark)

Type mapping when reading data from OceanBase Oracle mode:

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">OceanBase Type</th>
                <th class="text-left" style="width: 30%">Spark SQL Type</th>
                <th class="text-left" style="width: 40%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>NUMBER(0)</td>
                <td>DecimalType</td>
                <td>When precision and scale are not specified</td>
            </tr>
            <tr>
                <td>NUMBER(p,s)</td>
                <td>DecimalType(p,s)</td>
                <td></td>
            </tr>
            <tr>
                <td>BINARY_FLOAT</td>
                <td>FloatType</td>
                <td></td>
            </tr>
            <tr>
                <td>BINARY_DOUBLE</td>
                <td>DoubleType</td>
                <td></td>
            </tr>
            <tr>
                <td>VARCHAR2(n)</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>CLOB</td>
                <td>StringType</td>
                <td></td>
            </tr>
            <tr>
                <td>RAW(n)</td>
                <td>BinaryType</td>
                <td></td>
            </tr>
            <tr>
                <td>DATE</td>
                <td>DateType</td>
                <td></td>
            </tr>
            <tr>
                <td>TIMESTAMP</td>
                <td>TimestampType</td>
                <td></td>
            </tr>
            <tr>
                <td>TIMESTAMP WITH TIME ZONE</td>
                <td>TimestampType</td>
                <td>Supported under specific timezone configurations</td>
            </tr>
        </tbody>
    </table>
</div>

#### Writing Data (Spark → OceanBase)

Type mapping when writing data to existing OceanBase Oracle mode tables:

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">Spark SQL Type</th>
                <th class="text-left" style="width: 30%">OceanBase Type</th>
                <th class="text-left" style="width: 40%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>BooleanType</td>
                <td>NUMBER(1)</td>
                <td></td>
            </tr>
            <tr>
                <td>ByteType</td>
                <td>NUMBER(3)</td>
                <td></td>
            </tr>
            <tr>
                <td>ShortType</td>
                <td>NUMBER(5)</td>
                <td></td>
            </tr>
            <tr>
                <td>IntegerType</td>
                <td>NUMBER(10)</td>
                <td></td>
            </tr>
            <tr>
                <td>LongType</td>
                <td>NUMBER(19)</td>
                <td></td>
            </tr>
            <tr>
                <td>FloatType</td>
                <td>BINARY_FLOAT</td>
                <td></td>
            </tr>
            <tr>
                <td>DoubleType</td>
                <td>BINARY_DOUBLE</td>
                <td></td>
            </tr>
            <tr>
                <td>DecimalType(p,s)</td>
                <td>NUMBER(p,s)</td>
                <td></td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>VARCHAR2 / CLOB</td>
                <td>Depends on the actual target column type</td>
            </tr>
            <tr>
                <td>BinaryType</td>
                <td>RAW</td>
                <td></td>
            </tr>
            <tr>
                <td>DateType</td>
                <td>DATE</td>
                <td></td>
            </tr>
            <tr>
                <td>TimestampType</td>
                <td>TIMESTAMP</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>

#### Important Notes

1. **Complex type tables must be pre-created**: Create tables with complex types directly in OceanBase using SQL before reading or writing data through Spark.
2. **Nested array limitation**: ARRAY types support up to 6 levels of nesting, such as `INT[][]` or `INT[][][]`.
3. **JSON type handling**: JSON data is represented as StringType in Spark. Ensure the string content is valid JSON format when writing.
4. **ENUM and SET types**: Represented as StringType in Spark. When writing, values must conform to the enumeration or set values defined in the table.

