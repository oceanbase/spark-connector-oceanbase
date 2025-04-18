# OceanBase Spark Catalog

[English](spark-catalog-oceanbase.md) | 简体中文

OceanBase Spark Connector 从 1.1 版本开始全面支持 Spark Catalog，这为用户在 Spark 中使用 OceanBase 提供了更便捷和高效的解决方案。
通过使用 Spark Catalog，用户能够以更加简洁和一致的方式访问和操作 OceanBase 数据库。

## 当前支持功能

- 目前仅支持OceanBase MySQL模式。
- 支持Spark自适应分区并行读取OceanBase（基于JDBC）。
  - 谓词下推支持
- 支持通过旁路导入的方式写OceanBase
- 支持通过JDBC的方式写OceanBase。
  - 对于主键表，支持以upsert的方式写入。
    - MySQL模式基于：INSERT ... ON DUPLICATE KEY UPDATE 语法
  - 对于非主键表，则通过insert into写入
- 支持通过Spark-SQL管理OceanBase中的数据库和表，包括：show databases、show tables、drop table、drop database等语法支持。
  - 支持CTAS语法创建和写入OceanBase表。

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
                <td>1.1 及以后的版本</td>
                <td style="word-wrap: break-word;">3.1 ~ 3.5</td>
                <td>
                  <ul>
                    <li>JDBC: 3.x, 4.x</li>
                    <li>旁路导入: 4.2.x及以后的版本</li>
                  </ul>
                </td>
                <td>8</td>
                <td>2.12</td>
            </tr>
        </tbody>
    </table>
</div>

- 注意：如果需要基于其他 scala 版本构建的程序包, 您可以通过源码构建的方式获得程序包

## 获取程序包

您可以在 [Releases 页面](https://github.com/oceanbase/spark-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/spark-connector-oceanbase) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>spark-connector-oceanbase-3.4_2.12</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

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

当然您也可以通过源码构建的方式获得程序包。
- 默认以scala 2.12版本进行构建
- 编译成功后，会在各个版本对应的模块下的target目录生成目标 jar 包，如：spark-connector-oceanbase-3.4_2.12-1.0-SNAPSHOT.jar。 将此文件复制到 Spark 的 ClassPath 中即可使用 spark-connector-oceanbase。

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -DskipTests
```

- 如果需要其他 scala 版本，请参考下面以 scala 2.13版本构建命令

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.13.15 -Dscala.binary.version=2.13 -DskipTests
```

## 配置方式

启动 Spark-SQL CLI时设置OceanBase Catalog相关参数

```shell
./bin/spark-sql \
--conf "spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog" \
--conf "spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881" \
--conf "spark.sql.catalog.your_catalog_name.username=root@test" \
--conf "spark.sql.catalog.your_catalog_name.password=******" \
--conf "spark.sql.catalog.your_catalog_name.schema-name=test" \
--conf "spark.sql.defaultCatalog=your_catalog_name"
```

当然也可以将相关参数配置到spark的配置文件中，默认为spark-defaults.conf，如下所示，然后直接启动 Spark-SQL CLI.

```shell
spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog
spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881
spark.sql.catalog.your_catalog_name.username=root@test
spark.sql.catalog.your_catalog_name.password=******
spark.sql.catalog.your_catalog_name.schema-name=test
spark.sql.defaultCatalog=your_catalog_name
```

对于DataFrame API可以通过下述方式配置使用OceanBase Catalog：

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

## 使用示例

### 通过Spark-SQL管理OceanBase中的数据库和表

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

### 读取

```sql
SELECT * FROM test.test;
```

### 写入

以从Hive同步数据到OceanBase为例

#### 准备工作

创建对应的Hive表和OceanBase表，为数据同步做准备

- 通过${SPARK_HOME}/bin/spark-sql命令，开启spark-sql

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

- 连接到OceanBase

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

#### 通过JDBC方式进行数据同步

```sql
insert into table test.orders
select * from spark_catalog.default.orders;
```

#### 通过旁路导入方式进行数据同步

将下列旁路导入相关参数添加到spark的配置文件中，默认为spark-defaults.conf，如下所示，然后重新启动 Spark-SQL CLI.

```shell
spark.sql.catalog.your_catalog_name=com.oceanbase.spark.catalog.OceanBaseCatalog
spark.sql.catalog.your_catalog_name.url=jdbc:mysql://localhost:2881
spark.sql.catalog.your_catalog_name.username=root@test
spark.sql.catalog.your_catalog_name.password=******
spark.sql.catalog.your_catalog_name.schema-name=test
spark.sql.defaultCatalog=your_catalog_name

# 旁路导入相关参数
spark.sql.catalog.your_catalog_name.direct-load.enabled=true;
spark.sql.catalog.your_catalog_name.direct-load.host=localhost;
spark.sql.catalog.your_catalog_name.direct-load.rpc-port=2882;
```

```sql
insert into table test.orders
select * from spark_catalog.default.orders;
```

## 配置

### 通用配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 10%">是否必须</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>spark.sql.catalog.your_catalog_name</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>设置 Catalog 提供者的类名, 对于 OceanBase 来说唯一的有效值为 com.oceanbase.spark.catalog.OceanBaseCatalog。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.url</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接到OceanBase的 JDBC url。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.username</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>用户名。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.password</td>
                <td>是</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>密码。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.driver</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>用于连接到此 URL 的 JDBC 驱动程序的类名。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.schema-name</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>设置该OceanBase Catalog默认 schema。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.max-records-per-partition</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>Int</td>
                <td>控制Spark读取OB时，最多多少条数据作为一个Spark分区。默认为空，此时Spark会根据数据量自动计算出一个合理的数值。注意：一般情况下不建议设置该参数。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.parallel-hint-degree</td>
                <td>否</td>
                <td style="word-wrap: break-word;">1</td>
                <td>Int</td>
                <td>Spark读取OB时，Spark下发到OB的SQL会自动带上PARALLEL Hint。通过该参数可以调整其并行度，默认为1。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.statistics-parallel-hint-degree</td>
                <td>否</td>
                <td style="word-wrap: break-word;">4</td>
                <td>Int</td>
                <td>通过向生成的 SQL 添加 /*+ PARALLEL(N) */ hint 来控制统计查询（例如 COUNT、MIN、MAX）的并行级别。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.string-as-varchar-length</td>
                <td>否</td>
                <td style="word-wrap: break-word;">1024</td>
                <td>Int</td>
                <td>通过Spark-SQL创建OceanBase表时，映射 Spark String 类型到 OceanBase VARCHAR 类型的长度。默认值：1024。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-string-as-text</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>当该选项为 true 时，通过 Spark-SQL 创建 OceanBase 表时，spark 的 String 类型会转换为 OceanBase 的 text 类型。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-spark-varchar-datatype</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>当该选项为 true 时，OceanBase 的 VARCHAR 类型将会被转换为 spark 的 VARCHAR 类型。需要注意的是，spark VARCHAR 类型是一个实验性的功能。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.enable-always-nullable</td>
                <td>否</td>
                <td style="word-wrap: break-word;">true</td>
                <td>Boolean</td>
                <td>在模式推断过程中强制将所有字段标记为可空，忽略数据库元数据中的非空约束。用于处理元数据不完整或包含隐性空值的数据源。</td>
            </tr>
            <tr>
                <td>spark.sql.defaultCatalog</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>设置 Spark SQL默认 catalog。</td>
            </tr>
        </tbody>
    </table>
</div>

### 旁路导入配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 10%">是否必须</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.enabled</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启旁路导入写入。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.host</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>旁路导入用到的host地址。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.rpc-port</td>
                <td>否</td>
                <td>2882</td>
                <td>Integer</td>
                <td>旁路导入用到的rpc端口。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.username</td>
                <td>否</td>
                <td></td>
                <td>String</td>
                <td>旁路导入用户名。如果不指定该配置，则使用jdbc用户名。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.parallel</td>
                <td>否</td>
                <td>8</td>
                <td>Integer</td>
                <td>旁路导入服务端的并发度。该参数决定了服务端使用多少cpu资源来处理本次导入任务。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.batch-size</td>
                <td>否</td>
                <td>10240</td>
                <td>Integer</td>
                <td>一次写入OceanBase的批大小。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.max-error-rows</td>
                <td>否</td>
                <td>0</td>
                <td>Long</td>
                <td>旁路导入任务最大可容忍的错误行数目。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.dup-action</td>
                <td>否</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>旁路导入任务中主键重复时的处理策略。可以是 <code>STOP_ON_DUP</code>（本次导入失败），<code>REPLACE</code>（替换）或 <code>IGNORE</code>（忽略）。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.timeout</td>
                <td>否</td>
                <td>7d</td>
                <td>Duration</td>
                <td>旁路导入任务的超时时间。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.heartbeat-timeout</td>
                <td>否</td>
                <td>60s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳超时时间。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.heartbeat-interval</td>
                <td>否</td>
                <td>10s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳间隔时间。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.load-method</td>
                <td>否</td>
                <td>full</td>
                <td>String</td>
                <td>旁路导入导入模式：<code>full</code>, <code>inc</code>, <code>inc_replace</code>。
                <ul>
                    <li><code>full</code>：全量旁路导入，默认值。</li>
                    <li><code>inc</code>：普通增量旁路导入，会进行主键冲突检查，observer-4.3.2及以上支持，暂时不支持direct-load.dup-action为REPLACE。</li>
                    <li><code>inc_replace</code>: 特殊replace模式的增量旁路导入，不会进行主键冲突检查，直接覆盖旧数据（相当于replace的效果），direct-load.dup-action参数会被忽略，observer-4.3.2及以上支持。</li>
                </ul>
                </td>
            </tr>
        </tbody>
    </table>
</div>

