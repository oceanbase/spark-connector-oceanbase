# OceanBase Spark Catalog

English | [简体中文](spark-catalog-oceanbase_cn.md)

OceanBase Spark Connector fully supports Spark Catalog since version 1.1, which provides a more convenient and efficient solution for users to use OceanBase in Spark.
By using Spark Catalog, users can access and operate OceanBase databases in a more concise and consistent way.

## Currently supported features

- Currently only supports OceanBase MySQL mode.
- Supports Spark adaptive partitioning and parallel reading of OceanBase (via JDBC).
  - Predicate pushdown support
- Supports writing OceanBase through bypass import
- Supports writing OceanBase through JDBC.
  - For primary key tables, supports writing in upsert mode.
    - MySQL mode is based on: INSERT ... ON DUPLICATE KEY UPDATE syntax
  - For non-primary key tables, write through insert into
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
                <td>spark.sql.catalog.your_catalog_name.jdbc.query-timeout-hint-degree</td>
                <td>否</td>
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
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>The direct-load's username. If this configuration is not specified, the jdbc username is used.</td>
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

