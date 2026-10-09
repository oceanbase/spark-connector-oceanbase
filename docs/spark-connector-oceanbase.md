# Spark Connector OceanBase

English | [简体中文](spark-connector-oceanbase_cn.md)

Spark OceanBase Connector can support reading data stored in OceanBase through Spark, and also supports writing data to OceanBase through Spark.

|           |   Read    |         Write         |
|-----------|-----------|-----------------------|
| DataFrame | JDBC、OBKV | JDBC、Direct Load、OBKV |
| SQL       | JDBC、OBKV | JDBC、Direct Load、OBKV |

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
                <td>
                  <ul>
                    <li>JDBC: 3.x, 4.x</li>
                    <li>Direct Load: 4.2.x or later versions</li>
                  </ul>
                </td>
                <td>8</td>
                <td>2.12</td>
            </tr>
            <tr>
                <td>1.5</td>
                <td style="word-wrap: break-word;">2.4, 3.1 ~ 3.5</td>
                <td>
                  <ul>
                    <li>JDBC/OBKV: 3.x, 4.x</li>
                    <li>Direct Load: 4.2.x or later versions</li>
                  </ul>
                </td>
                <td>8</td>
                <td>2.12</td>
            </tr>
            <tr>
                <td>1.5</td>
                <td style="word-wrap: break-word;">4.0</td>
                <td>
                  <ul>
                    <li>JDBC/OBKV: 3.x, 4.x</li>
                    <li>Direct Load: 4.2.x or later versions</li>
                  </ul>
                </td>
                <td>17</td>
                <td>2.13</td>
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

- If you need a package built based on other Scala versions, refer to the command below to build based on Scala 2.11.

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.11.12 -Dscala.binary.version=2.11 -DskipTests
```

## Usage Examples

### Read

#### Use Spark-SQL.

```sql
CREATE TEMPORARY VIEW spark_oceanbase
USING oceanbase
OPTIONS(
  "url"= "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
  "schema-name"="test",
  "table-name"="test",
  "username"="root",
  "password"="123456"
);

SELECT * FROM spark_oceanbase;
```

#### Use DataFrame

```scala
val oceanBaseSparkDF = spark.read.format("OceanBase")
  .option("url", "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
  .option("username", "root")
  .option("password", "123456")
  .option("table-name", "test")
  .option("schema-name", "test")
  .load()

oceanBaseSparkDF.show(5)
```

#### Use OBKV

Reading through OBKV requires the table to have a primary key, and supports server-side predicate pushdown.

```scala
val oceanBaseSparkDF = spark.read.format("oceanbase")
  .option("url", "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
  .option("username", "root@test")
  .option("password", "123456")
  .option("schema-name", "test")
  .option("table-name", "orders")
  .option("obkv.enabled", "true")
  .option("obkv.param-url", "http://localhost:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster")
  .option("obkv.full-user-name", "root@test#obcluster")
  .load()

oceanBaseSparkDF.show(5)
```

- Note: If you declare the schema manually through `.schema(...)`, you also need to set `obkv.primary-key` to the comma-separated primary key column names.

### Write

Take synchronizing data from Hive to OceanBase as an example.

#### Preparation

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

#### Based on JDBC

##### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_jdbc
USING oceanbase
OPTIONS(
  "url"= "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
  "schema-name"="test",
  "table-name"="orders",
  "username"="root@test",
  "password"=""
);

insert into table test_jdbc
select * from test.orders;

insert overwrite table test_jdbc
select * from test.orders;
```

##### DataFrame

```scala
val df = spark.sql("select * from test.orders")

import org.apache.spark.sql.SaveMode
df.write
  .format("oceanbase")
  .mode(saveMode = SaveMode.Append)
  .option("url", "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
  .option("username", "root")
  .option("password", "123456")
  .option("table-name", "orders")
  .option("schema-name", "test")
  .save()
```

#### Based on direct-load

##### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_direct
USING oceanbase
OPTIONS(
  "url"="jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
  "schema-name"="test",
  "table-name"="orders",
  "username"="root@test",
  "password"="123456",
  "direct-load.enabled" = true,
  "direct-load.host" = "localhost",
  "direct-load.rpc-port" = "2882"
);

insert into table test_direct
select * from test.orders;

insert overwrite table test_direct
select * from test.orders;
```

##### DataFrame

```scala
val df = spark.sql("select * from test.orders")

import org.apache.spark.sql.SaveMode
df.write
  .format("oceanbase")
  .mode(saveMode = SaveMode.Append)
  .option("url", "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
  .option("username", "root")
  .option("password", "123456")
  .option("table-name", "orders")
  .option("schema-name", "test")
  .option("direct-load.enabled", "true")
  .option("direct-load.host", "localhost")
  .option("direct-load.rpc-port", "2882")
  .save()
```

#### Based on OBKV

Writing through OBKV uses OceanBase's TableAPI and supports insert, insertOrUpdate, replace and put semantics through `obkv.dup-action`.

##### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_obkv
USING oceanbase
OPTIONS(
  "url"="jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
  "schema-name"="test",
  "table-name"="orders",
  "username"="root@test",
  "password"="123456",
  "obkv.enabled"="true",
  "obkv.param-url"="http://localhost:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster",
  "obkv.full-user-name"="root@test#obcluster",
  "obkv.primary-key"="order_id"
);

insert into table test_obkv
select * from test.orders;

insert overwrite table test_obkv
select * from test.orders;
```

##### DataFrame

```scala
val df = spark.sql("select * from test.orders")

import org.apache.spark.sql.SaveMode
df.write
  .format("oceanbase")
  .mode(saveMode = SaveMode.Append)
  .option("url", "jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
  .option("username", "root@test")
  .option("password", "123456")
  .option("table-name", "orders")
  .option("schema-name", "test")
  .option("obkv.enabled", "true")
  .option("obkv.param-url", "http://localhost:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster")
  .option("obkv.full-user-name", "root@test#obcluster")
  .option("obkv.primary-key", "order_id")
  .save()
```

- Note: OBKV write currently supports the following column types: INT, BIGINT, DOUBLE, VARCHAR, CHAR, TIMESTAMP, DATETIME. TINYINT, SMALLINT and FLOAT are read-only. DECIMAL, DATE, TEXT and VARBINARY are not supported.

## Configuration

### General configuration

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>url</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The connection URL.</td>
            </tr>
            <tr>
                <td>username</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The connection username like 'root@sys'.</td>
            </tr>
            <tr>
                <td>tenant-name</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The tenant name.</td>
            </tr>
            <tr>
                <td>password</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The password.</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The schema name or database name.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>The table name.</td>
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
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>direct-load.enabled</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Enable direct-load writing.</td>
            </tr>
            <tr>
                <td>direct-load.host</td>
                <td></td>
                <td>String</td>
                <td>Hostname used in direct-load.</td>
            </tr>
            <tr>
                <td>direct-load.rpc-port</td>
                <td>2882</td>
                <td>Integer</td>
                <td>Rpc port number used in direct-load.</td>
            </tr>
            <tr>
                <td>direct-load.write-thread-num</td>
                <td>8</td>
                <td>Integer</td>
                <td>The number of threads to use when direct-load client are writing.</td>
            </tr>
            <tr>
                <td>direct-load.parallel</td>
                <td>8</td>
                <td>Integer</td>
                <td>The parallel of the direct-load server. This parameter determines how much CPU resources the server uses to process this import task.</td>
            </tr>
            <tr>
                <td>direct-load.batch-size</td>
                <td>10240</td>
                <td>Integer</td>
                <td>The size of the batch that is written to the OceanBase at one time.</td>
            </tr>
            <tr>
                <td>direct-load.max-error-rows</td>
                <td>0</td>
                <td>Long</td>
                <td>Maximum tolerable number of error rows.</td>
            </tr>
            <tr>
                <td>direct-load.dup-action</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>Action when there is duplicated record of direct-load task. Can be <code>STOP_ON_DUP</code>, <code>REPLACE</code> or <code>IGNORE</code>.</td>
            </tr>
            <tr>
                <td>direct-load.timeout</td>
                <td>7d</td>
                <td>Duration</td>
                <td>The timeout for direct-load task.</td>
            </tr>
            <tr>
                <td>direct-load.heartbeat-timeout</td>
                <td>60s</td>
                <td>Duration</td>
                <td>Client heartbeat timeout in direct-load task.</td>
            </tr>
            <tr>
                <td>direct-load.heartbeat-interval</td>
                <td>10s</td>
                <td>Duration</td>
                <td>Client heartbeat interval in direct-load task.</td>
            </tr>
            <tr>
                <td>direct-load.load-method</td>
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
        </tbody>
    </table>
</div>

### OBKV configuration

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>obkv.enabled</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Enable OBKV read/write mode.</td>
            </tr>
            <tr>
                <td>obkv.param-url</td>
                <td></td>
                <td>String</td>
                <td>The OceanBase config server URL for direct connection mode (required when obkv.odp-mode is false).</td>
            </tr>
            <tr>
                <td>obkv.full-user-name</td>
                <td></td>
                <td>String</td>
                <td>The full user name for OBKV connection, format: user@tenant#cluster.</td>
            </tr>
            <tr>
                <td>obkv.password</td>
                <td></td>
                <td>String</td>
                <td>The password for OBKV connection. If not set, reuses the global password.</td>
            </tr>
            <tr>
                <td>obkv.sys-user-name</td>
                <td></td>
                <td>String</td>
                <td>The system tenant user name for OBKV (optional).</td>
            </tr>
            <tr>
                <td>obkv.sys-password</td>
                <td></td>
                <td>String</td>
                <td>The system tenant password for OBKV (optional).</td>
            </tr>
            <tr>
                <td>obkv.odp-mode</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Whether to use ODP proxy for OBKV connection.</td>
            </tr>
            <tr>
                <td>obkv.odp-addr</td>
                <td></td>
                <td>String</td>
                <td>The ODP address for OBKV proxy mode.</td>
            </tr>
            <tr>
                <td>obkv.odp-port</td>
                <td>2882</td>
                <td>Integer</td>
                <td>The ODP port for OBKV proxy mode.</td>
            </tr>
            <tr>
                <td>obkv.batch-size</td>
                <td>1024</td>
                <td>Integer</td>
                <td>The batch size for OBKV read/write operations.</td>
            </tr>
            <tr>
                <td>obkv.rpc-connect-timeout</td>
                <td>5000</td>
                <td>Integer</td>
                <td>The RPC connect timeout in milliseconds for OBKV connections.</td>
            </tr>
            <tr>
                <td>obkv.rpc-execute-timeout</td>
                <td>10000</td>
                <td>Integer</td>
                <td>The RPC execute timeout in milliseconds for OBKV operations.</td>
            </tr>
            <tr>
                <td>obkv.operation-timeout</td>
                <td>10000</td>
                <td>Integer</td>
                <td>The operation timeout in milliseconds for OBKV client.</td>
            </tr>
            <tr>
                <td>obkv.dup-action</td>
                <td>INSERT_OR_UPDATE</td>
                <td>String</td>
                <td>The action when there is a duplicate record during OBKV write. Can be <code>INSERT_OR_UPDATE</code>, <code>INSERT</code>, <code>REPLACE</code> or <code>PUT</code>.</td>
            </tr>
            <tr>
                <td>obkv.read-consistency</td>
                <td>STRONG</td>
                <td>String</td>
                <td>The read consistency level for OBKV queries. Can be <code>STRONG</code> or <code>WEAK</code>.</td>
            </tr>
            <tr>
                <td>obkv.primary-key</td>
                <td></td>
                <td>String</td>
                <td>The primary key column names for OBKV, comma-separated. Required in non-Catalog mode.</td>
            </tr>
        </tbody>
    </table>
</div>

### JDBC configuration

- This Connector is implemented based on [JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).
  - For more configuration items, see: [JDBC To Other Databases#Data Source Option](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option)
- Support OceanBase MySQL and Oracle modes:
  - For MySQL mode, you need to add the `MySQL Connector/J` driver to Spark's CLASSPATH
  - For Oracle mode, you need to add the `OceanBase Connector/J` driver to Spark's CLASSPATH
- The `url` option accepts multiple comma-separated JDBC URLs, in which case connection failures automatically fail over to the remaining URLs.

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">Option</th>
                <th class="text-left" style="width: 10%">Default</th>
                <th class="text-left" style="width: 15%">Type</th>
                <th class="text-left" style="width: 50%">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>jdbc.connection.max-retries</td>
                <td>3</td>
                <td>Integer</td>
                <td>Maximum number of attempts when opening a JDBC connection, including the initial attempt.</td>
            </tr>
            <tr>
                <td>jdbc.connection.retry-interval</td>
                <td>1s</td>
                <td>Duration</td>
                <td>Initial retry interval when opening a JDBC connection.</td>
            </tr>
            <tr>
                <td>jdbc.connection.failed-url-cooldown</td>
                <td>60s</td>
                <td>Duration</td>
                <td>Cooldown before a JDBC URL that failed to connect is treated as healthy again.</td>
            </tr>
        </tbody>
    </table>
</div>

