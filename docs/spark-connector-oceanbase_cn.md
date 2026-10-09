# Spark Connector OceanBase

[English](spark-connector-oceanbase.md) | 简体中文

Spark OceanBase Connector 可以支持通过 Spark 读取 OceanBase 中存储的数据，也支持通过 Spark 写入数据到 OceanBase。

|           |   Read    |     Write      |
|-----------|-----------|----------------|
| DataFrame | JDBC、OBKV | JDBC、旁路导入、OBKV |
| SQL       | JDBC、OBKV | JDBC、旁路导入、OBKV |

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
                <td>
                  <ul>
                    <li>JDBC: 3.x, 4.x</li>
                    <li>旁路导入: 4.2.x及以后的版本</li>
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
                    <li>旁路导入: 4.2.x及以后的版本</li>
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
                    <li>旁路导入: 4.2.x及以后的版本</li>
                  </ul>
                </td>
                <td>17</td>
                <td>2.13</td>
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

- 如果需要其他 scala 版本，请参考下面以 scala 2.11版本构建命令

```shell
git clone https://github.com/oceanbase/spark-connector-oceanbase.git
cd spark-connector-oceanbase
mvn clean package -Dscala.version=2.11.12 -Dscala.binary.version=2.11 -DskipTests
```

## 使用示例

### 读取

#### 通过Spark-SQL

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

#### 通过DataFrame

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

#### 通过OBKV方式

通过 OBKV 读取要求表具有主键，并支持服务端的谓词下推。

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

- 注意：如果通过 `.schema(...)` 手动声明 schema，还需要将 `obkv.primary-key` 设置为逗号分隔的主键列名。

### 写入

以从Hive同步数据到OceanBase为例

#### 准备工作

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

#### 通过JDBC方式

##### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_jdbc
USING oceanbase
OPTIONS(
  "url"="jdbc:mysql://localhost:2881/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false",
  "schema-name"="test",
  "table-name"="orders",
  "username"="root@test",
  "password"="123456"
);

insert into table test_jdbc
select * from test.orders;

insert overwrite table test_jdbc
select * from test.orders;
```

##### DataFrame

```scala
// 读取hive表test.orders
val df = spark.sql("select * from test.orders")

// 写入到OceanBase使用DataFrame
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

#### 通过旁路导入方式

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
// 读取hive表test.orders
val df = spark.sql("select * from test.orders")

// 写入到OceanBase使用DataFrame
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

#### 通过OBKV方式

通过 OBKV 写入使用 OceanBase 的 TableAPI，并通过 `obkv.dup-action` 支持 insert、insertOrUpdate、replace 和 put 语义。

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
// 读取hive表test.orders
val df = spark.sql("select * from test.orders")

// 通过OBKV写入到OceanBase
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

- 注意：OBKV 写入目前支持以下列类型：INT、BIGINT、DOUBLE、VARCHAR、CHAR、TIMESTAMP、DATETIME。TINYINT、SMALLINT 和 FLOAT 仅支持读取。暂不支持 DECIMAL、DATE、TEXT 和 VARBINARY。

## 配置

### 通用配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>url</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接到OceanBase的 JDBC url.</td>
            </tr>
            <tr>
                <td>username</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接用户名。</td>
            </tr>
            <tr>
                <td>password</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接密码。</td>
            </tr>
            <tr>
                <td>schema-name</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>连接的 schema 名或 db 名。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td style="word-wrap: break-word;"></td>
                <td>String</td>
                <td>表名。</td>
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
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>direct-load.enabled</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启旁路导入写入。</td>
            </tr>
            <tr>
                <td>direct-load.host</td>
                <td></td>
                <td>String</td>
                <td>旁路导入用到的host地址。</td>
            </tr>
            <tr>
                <td>direct-load.rpc-port</td>
                <td>2882</td>
                <td>Integer</td>
                <td>旁路导入用到的rpc端口。</td>
            </tr>
            <tr>
                <td>direct-load.write-thread-num</td>
                <td>8</td>
                <td>Integer</td>
                <td>旁路导入客户端写入时使用的线程数。</td>
            </tr>
            <tr>
                <td>direct-load.parallel</td>
                <td>8</td>
                <td>Integer</td>
                <td>旁路导入服务端的并发度。该参数决定了服务端使用多少cpu资源来处理本次导入任务。</td>
            </tr>
            <tr>
                <td>direct-load.batch-size</td>
                <td>10240</td>
                <td>Integer</td>
                <td>一次写入OceanBase的批大小。</td>
            </tr>
            <tr>
                <td>direct-load.max-error-rows</td>
                <td>0</td>
                <td>Long</td>
                <td>旁路导入任务最大可容忍的错误行数目。</td>
            </tr>
            <tr>
                <td>direct-load.dup-action</td>
                <td>REPLACE</td>
                <td>String</td>
                <td>旁路导入任务中主键重复时的处理策略。可以是 <code>STOP_ON_DUP</code>（本次导入失败），<code>REPLACE</code>（替换）或 <code>IGNORE</code>（忽略）。</td>
            </tr>
            <tr>
                <td>direct-load.timeout</td>
                <td>7d</td>
                <td>Duration</td>
                <td>旁路导入任务的超时时间。</td>
            </tr>
            <tr>
                <td>direct-load.heartbeat-timeout</td>
                <td>60s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳超时时间。</td>
            </tr>
            <tr>
                <td>direct-load.heartbeat-interval</td>
                <td>10s</td>
                <td>Duration</td>
                <td>旁路导入任务客户端的心跳间隔时间。</td>
            </tr>
            <tr>
                <td>direct-load.load-method</td>
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

### OBKV配置项

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>obkv.enabled</td>
                <td>false</td>
                <td>Boolean</td>
                <td>是否开启 OBKV 读写模式。</td>
            </tr>
            <tr>
                <td>obkv.param-url</td>
                <td></td>
                <td>String</td>
                <td>直连模式下 OceanBase 的 config server 地址（obkv.odp-mode 为 false 时必填）。</td>
            </tr>
            <tr>
                <td>obkv.full-user-name</td>
                <td></td>
                <td>String</td>
                <td>OBKV 连接使用的完整用户名，格式为 user@tenant#cluster。</td>
            </tr>
            <tr>
                <td>obkv.password</td>
                <td></td>
                <td>String</td>
                <td>OBKV 连接的密码。未设置时复用全局 password。</td>
            </tr>
            <tr>
                <td>obkv.sys-user-name</td>
                <td></td>
                <td>String</td>
                <td>OBKV 系统租户用户名（可选）。</td>
            </tr>
            <tr>
                <td>obkv.sys-password</td>
                <td></td>
                <td>String</td>
                <td>OBKV 系统租户密码（可选）。</td>
            </tr>
            <tr>
                <td>obkv.odp-mode</td>
                <td>false</td>
                <td>Boolean</td>
                <td>OBKV 连接是否使用 ODP 代理。</td>
            </tr>
            <tr>
                <td>obkv.odp-addr</td>
                <td></td>
                <td>String</td>
                <td>ODP 代理模式下 ODP 的地址。</td>
            </tr>
            <tr>
                <td>obkv.odp-port</td>
                <td>2882</td>
                <td>Integer</td>
                <td>ODP 代理模式下 ODP 的端口。</td>
            </tr>
            <tr>
                <td>obkv.batch-size</td>
                <td>1024</td>
                <td>Integer</td>
                <td>OBKV 读写操作的批大小。</td>
            </tr>
            <tr>
                <td>obkv.rpc-connect-timeout</td>
                <td>5000</td>
                <td>Integer</td>
                <td>OBKV 连接的 RPC 连接超时时间，单位毫秒。</td>
            </tr>
            <tr>
                <td>obkv.rpc-execute-timeout</td>
                <td>10000</td>
                <td>Integer</td>
                <td>OBKV 操作的 RPC 执行超时时间，单位毫秒。</td>
            </tr>
            <tr>
                <td>obkv.operation-timeout</td>
                <td>10000</td>
                <td>Integer</td>
                <td>OBKV 客户端的操作超时时间，单位毫秒。</td>
            </tr>
            <tr>
                <td>obkv.dup-action</td>
                <td>INSERT_OR_UPDATE</td>
                <td>String</td>
                <td>OBKV 写入时主键重复的处理策略。可以是 <code>INSERT_OR_UPDATE</code>、<code>INSERT</code>、<code>REPLACE</code> 或 <code>PUT</code>。</td>
            </tr>
            <tr>
                <td>obkv.read-consistency</td>
                <td>STRONG</td>
                <td>String</td>
                <td>OBKV 查询的读一致性级别。可以是 <code>STRONG</code> 或 <code>WEAK</code>。</td>
            </tr>
            <tr>
                <td>obkv.primary-key</td>
                <td></td>
                <td>String</td>
                <td>OBKV 的主键列名，多个列名用逗号分隔。非 Catalog 模式下必填。</td>
            </tr>
        </tbody>
    </table>
</div>

### JDBC配置项

- 此Connector在[JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)的基础上实现。
  - 更多配置项见：[JDBC To Other Databases#Data Source Option](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option)
- 支持OceanBase的MySQL和Oracle模式：
  - 对于MySQL模式需要添加`MySQL Connector/J`驱动到Spark的CLASSPATH
  - 对于Oracle模式需要添加`OceanBase Connector/J`驱动到Spark的CLASSPATH
- `url` 参数支持配置多个逗号分隔的 JDBC URL，当连接失败时会自动故障转移到其余可用的 URL。

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">参数名</th>
                <th class="text-left" style="width: 10%">默认值</th>
                <th class="text-left" style="width: 15%">类型</th>
                <th class="text-left" style="width: 50%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>jdbc.connection.max-retries</td>
                <td>3</td>
                <td>Integer</td>
                <td>建立 JDBC 连接时的最大尝试次数，包含首次尝试。</td>
            </tr>
            <tr>
                <td>jdbc.connection.retry-interval</td>
                <td>1s</td>
                <td>Duration</td>
                <td>建立 JDBC 连接时的初始重试间隔。</td>
            </tr>
            <tr>
                <td>jdbc.connection.failed-url-cooldown</td>
                <td>60s</td>
                <td>Duration</td>
                <td>连接失败的 JDBC URL 被重新视为可用之前的冷却时间。</td>
            </tr>
        </tbody>
    </table>
</div>

