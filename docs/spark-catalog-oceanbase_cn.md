# OceanBase Spark Catalog

[English](spark-catalog-oceanbase.md) | 简体中文

OceanBase Spark Connector 从 1.1 版本开始全面支持 Spark Catalog，这为用户在 Spark 中使用 OceanBase 提供了更便捷和高效的解决方案。
通过使用 Spark Catalog，用户能够以更加简洁和一致的方式访问和操作 OceanBase 数据库。

## 当前支持功能

- 目前仅支持OceanBase MySQL模式。
- 支持Spark自适应分区并行读取OceanBase（基于JDBC）。
  - 谓词下推支持
- 支持通过旁路导入的方式写OceanBase。
- 支持通过JDBC的方式写OceanBase。
  - 对于主键表，支持以upsert的方式写入。
    - MySQL模式基于：`INSERT INTO ... ON DUPLICATE KEY UPDATE` 语法。
  - 对于非主键表，则通过 `INSERT INTO` 写入。
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

旁路导入注意事项：
- 旁路导入任务期间会**锁表**，锁表期间不能写入数据、不能进行DDL变更，但可以进行数据查询。

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
                <td>spark.sql.catalog.your_catalog_name.jdbc.fetch-size</td>
                <td>否</td>
                <td style="word-wrap: break-word;">100</td>
                <td>Int</td>
                <td>JDBC 读取时抓取大小，决定每次数据库往返获取的数据行数。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.batch-size</td>
                <td>否</td>
                <td style="word-wrap: break-word;">1024</td>
                <td>Int</td>
                <td>JDBC 写入批处理大小，决定每次数据库往返批量插入的数据行数。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.max-records-per-partition</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>Int</td>
                <td>控制Spark读取OB时，最多多少条数据作为一个Spark分区。默认为空，此时Spark会根据数据量自动计算出一个合理的数值。注意：一般情况下不建议设置该参数。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.parallel-hint-degree</td>
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
                <td>spark.sql.catalog.your_catalog_name.jdbc.partition-compute-parallelism</td>
                <td>否</td>
                <td style="word-wrap: break-word;">32</td>
                <td>Int</td>
                <td>控制分区计算的并行级别。此参数确定计算分区表分区时使用的线程数，主要通过并行 SQL 查询 OceanBase 分区统计信息来实现。该计算任务在 driver 节点运行，对于分区数量较多的表，设置更高的值可以显著提升性能。当指定的该参数值较大的时候，适当调大 driver 节点的 CPU 核数和内存，可以取得更好的性能。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.partition-compute-timeout-minutes</td>
                <td>否</td>
                <td style="word-wrap: break-word;">10</td>
                <td>Int</td>
                <td>分区计算的超时时间，单位为分钟。此参数控制在抛出超时异常之前等待分区计算完成的时长。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.query-timeout-hint-degree</td>
                <td>否</td>
                <td style="word-wrap: break-word;">-1</td>
                <td>Int</td>
                <td>通过向生成的 SQL 添加 /*+ query_timeout(n) */ hint 来控制查询超时时间。通过该参数可以设置超时时间，单位为微妙。默认为-1，表示不添加该Hint。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.disable-pk-table-use-where-partition</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>如果为true，则将禁止主键表使用 where 子句进行分区。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.{database}.{table}.partition-column</td>
                <td>否</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>您可以手动指定主键表分区列，否则将默认自动从主键列中选择一个。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.enable-autocommit</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>使用 jdbc 写入时，是否启用 autocommit 进行事务自动提交。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.use-insert-ignore</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>当启用时，使用 INSERT IGNORE 而不是 INSERT ... ON DUPLICATE KEY UPDATE 来处理主键冲突。INSERT IGNORE 会跳过具有重复键的行并继续处理，而 ON DUPLICATE KEY UPDATE 会用新值更新现有行。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.upsert-by-unique-key</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>当表同时拥有主键和唯一键索引时，此选项控制使用哪个键来进行冲突检测。如果设置为 true，则使用唯一键进行冲突检测，并更新除唯一键列之外的所有列（包括主键列）。如果设置为 false（默认值），则使用主键进行冲突检测，并更新除主键列之外的所有列。</td>
            </tr>
            <tr>
                <td>spark.sql.catalog.your_catalog_name.jdbc.optimize-decimal-string-comparison</td>
                <td>否</td>
                <td style="word-wrap: break-word;">false</td>
                <td>Boolean</td>
                <td>当该选项为 true 时，精度小于等于 19 的 DECIMAL(P, 0) 列将被转换为 BIGINT (LongType)，以避免与字符串字面量比较时的精度丢失问题。此优化可防止 Spark 将 String + DECIMAL 转换为 DOUBLE（对于大数字会丢失精度）。如果设置为 false（默认值），DECIMAL 列将保持为 DecimalType。注意：此优化仅适用于 scale = 0 的整数型 DECIMAL，且 precision <= 19（BIGINT 范围：-9223372036854775808 到 9223372036854775807）。</td>
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
                <td>spark.sql.catalog.your_catalog_name.direct-load.odp-mode</td>
                <td>否</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否通过ODP代理进行旁路导入。当设置为true时，将通过ODP代理（通常是2885端口）连接，此时会传递完整的用户名格式（如user@tenant#cluster）；当设置为false（默认）时，直连OBServer（通常是2882端口）。</td>
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
            <tr>
                <td>spark.sql.catalog.your_catalog_name.direct-load.dup-action</td>
                <td>否</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>旁路导入任务中主键重复时的处理策略。可以是 <code>STOP_ON_DUP</code>（本次导入失败），<code>REPLACE</code>（替换）或 <code>IGNORE</code>（忽略）。</td>
            </tr>
        </tbody>
    </table>
</div>

## 数据类型映射

本节介绍在读写 OceanBase 数据时，OceanBase 数据类型与 Spark SQL 数据类型之间的映射关系。

### MySQL 模式

#### 读取数据时（OceanBase → Spark）

从 OceanBase MySQL 模式读取数据时的类型映射：

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">OceanBase 类型</th>
                <th class="text-left" style="width: 30%">Spark SQL 类型</th>
                <th class="text-left" style="width: 40%">说明</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td colspan="3"><b>基础数据类型</b></td>
            </tr>
            <tr>
                <td>BIT</td>
                <td>LongType</td>
                <td>当 size != 1 时</td>
            </tr>
            <tr>
                <td>BIT</td>
                <td>BooleanType</td>
                <td>当 size = 1 时</td>
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
                <td colspan="3"><b>复杂数据类型</b></td>
            </tr>
            <tr>
                <td>ARRAY(type)</td>
                <td>ArrayType(对应类型)</td>
                <td>支持最多 6 层嵌套，支持的元素类型包括：INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING 等</td>
            </tr>
            <tr>
                <td>VECTOR(n)</td>
                <td>ArrayType(FloatType)</td>
                <td>n 表示向量维度</td>
            </tr>
            <tr>
                <td>MAP(keyType, valueType)</td>
                <td>MapType(keyType, valueType)</td>
                <td></td>
            </tr>
            <tr>
                <td>JSON</td>
                <td>StringType</td>
                <td>JSON 数据以字符串形式读取</td>
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

#### 写入数据时（Spark → OceanBase）

向已存在的 OceanBase MySQL 模式表写入数据时的类型映射：

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">Spark SQL 类型</th>
                <th class="text-left" style="width: 30%">OceanBase 类型</th>
                <th class="text-left" style="width: 40%">说明</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td colspan="3"><b>基础数据类型</b></td>
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
                <td>根据目标列的实际类型</td>
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
                <td colspan="3"><b>复杂数据类型</b></td>
            </tr>
            <tr>
                <td>ArrayType(IntegerType)</td>
                <td>INT[] / ARRAY(INT)</td>
                <td>表必须预先创建包含 ARRAY 类型的列</td>
            </tr>
            <tr>
                <td>ArrayType(FloatType)</td>
                <td>FLOAT[] / VECTOR(n)</td>
                <td>可以写入到 VECTOR 或 FLOAT ARRAY 列</td>
            </tr>
            <tr>
                <td>MapType(keyType, valueType)</td>
                <td>MAP(keyType, valueType)</td>
                <td>表必须预先创建包含 MAP 类型的列</td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>JSON</td>
                <td>JSON 数据以字符串形式写入</td>
            </tr>
            <tr>
                <td>StringType</td>
                <td>ENUM / SET</td>
                <td>写入的字符串值必须符合 ENUM/SET 定义</td>
            </tr>
        </tbody>
    </table>
</div>

### Oracle 模式

#### 读取数据时（OceanBase → Spark）

从 OceanBase Oracle 模式读取数据时的类型映射：

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">OceanBase 类型</th>
                <th class="text-left" style="width: 30%">Spark SQL 类型</th>
                <th class="text-left" style="width: 40%">说明</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>NUMBER(0)</td>
                <td>DecimalType</td>
                <td>未指定精度和标度时</td>
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
                <td>在特定时区配置下支持</td>
            </tr>
        </tbody>
    </table>
</div>

#### 写入数据时（Spark → OceanBase）

向已存在的 OceanBase Oracle 模式表写入数据时的类型映射：

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 30%">Spark SQL 类型</th>
                <th class="text-left" style="width: 30%">OceanBase 类型</th>
                <th class="text-left" style="width: 40%">说明</th>
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
                <td>根据目标列的实际类型</td>
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

#### 重要说明

1. **复杂类型表必须预先创建**：通过 SQL 直接在 OceanBase 中创建包含复杂类型的表，然后才能通过 Spark 读写数据。
2. **嵌套数组限制**：ARRAY 类型最多支持 6 层嵌套，例如 `INT[][]` 或 `INT[][][]`。
3. **JSON 类型处理**：JSON 数据在 Spark 中以 StringType 表示，写入时确保字符串内容是有效的 JSON 格式。
4. **ENUM 和 SET 类型**：在 Spark 中以 StringType 表示，写入时值必须符合表定义中的枚举或集合值。

