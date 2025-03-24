## Spark Connector OBKV HBase

[English](spark-connector-obkv-hbase.md) | 简体中文

本项目是一个 OBKV HBase 的 Spark Connector，可以在 Spark 中通过 [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java) 将数据写入到 OceanBase。

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

- 连接到 OceanBase

```sql
use test;
CREATE TABLE `htable1$family1`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
)
```

### Config Url 模式

#### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_obkv
USING `obkv-hbase`
OPTIONS(
  "url" = "http://localhost:8080/services?Action=ObRootServiceInfo&ObRegion=myob",
  "sys.username"= "root",
  "sys.password" = "password",
  "schema-name"="test",
  "table-name"="htable1",
  "username"="root@sys#myob",
  "password"="password",
  "schema"="{
    'order_id': {'cf': 'rowkey','col': 'order_id','type': 'int'},
    'order_date': {'cf': 'family1','col': 'order_date','type': 'timestamp'},
    'customer_name': {'cf': 'family1','col': 'customer_name','type': 'string'},
    'price': {'cf': 'family1','col': 'price','type': 'double'},
    'product_id': {'cf': 'family1','col': 'product_id','type': 'int'},
    'order_status': {'cf': 'family1','col': 'order_status','type': 'boolean'}
}"
);

insert into table test_obkv
select * from test.orders;
```

#### DataFrame

```scala
val df = spark.sql("select * from test.orders")

val schema: String =
  """
    |{
    |    "order_id": {"cf": "rowkey","col": "order_id","type": "int"},
    |    "order_date": {"cf": "family1","col": "order_date","type": "timestamp"},
    |    "customer_name": {"cf": "family1","col": "customer_name","type": "string"},
    |    "price": {"cf": "family1","col": "price","type": "double"},
    |    "product_id": {"cf": "family1","col": "product_id","type": "int"},
    |    "order_status": {"cf": "family1","col": "order_status","type": "boolean"}
    |}
    |""".stripMargin

df.write
  .format("obkv-hbase")
  .option("url", "http://localhost:8080/services?Action=ObRootServiceInfo&ObRegion=myob")
  .option("sys-username", "root")
  .option("sys-password", "password")
  .option("username", "root@sys#myob")
  .option("password", "password")
  .option("table-name", "htable1")
  .option("schema-name", "test")
  .option("schema", schema)
  .save()
```

### ODP模式

#### Spark-SQL

```sql
CREATE TEMPORARY VIEW test_obkv
USING `obkv-hbase`
OPTIONS(
  "odp-mode" = true,
  "odp-ip"= "root",
  "odp-port" = "password",
  "schema-name"="test",
  "table-name"="htable1",
  "username"="root@sys#myob",
  "password"="password",
  "schema"="{
    'order_id': {'cf': 'rowkey','col': 'order_id','type': 'int'},
    'order_date': {'cf': 'family1','col': 'order_date','type': 'timestamp'},
    'customer_name': {'cf': 'family1','col': 'customer_name','type': 'string'},
    'price': {'cf': 'family1','col': 'price','type': 'double'},
    'product_id': {'cf': 'family1','col': 'product_id','type': 'int'},
    'order_status': {'cf': 'family1','col': 'order_status','type': 'boolean'}
}"
);

insert into table test_obkv
select * from test.orders;
```

#### DataFrame

```scala
val df = spark.sql("select * from test.orders")

val schema: String =
  """
    |{
    |    "order_id": {"cf": "rowkey","col": "order_id","type": "int"},
    |    "order_date": {"cf": "family1","col": "order_date","type": "timestamp"},
    |    "customer_name": {"cf": "family1","col": "customer_name","type": "string"},
    |    "price": {"cf": "family1","col": "price","type": "double"},
    |    "product_id": {"cf": "family1","col": "product_id","type": "int"},
    |    "order_status": {"cf": "family1","col": "order_status","type": "boolean"}
    |}
    |""".stripMargin

df.write
  .format("obkv-hbase")
  .option("odp-mode", true)
  .option("odp-ip", "localhost")
  .option("odp-port", 2885)
  .option("username", "root@sys#myob")
  .option("password", "password")
  .option("schema-name", "test")
  .option("table-name", "htable1")
  .option("schema", schema)
  .save()
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
      <td>HBase 表名，注意在 OceanBase 中表名的结构是 <code>hbase_table_name$family_name</code>。</td>
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
     <td>schema</td>
     <td>Yes</td>
     <td></td>
     <td>String</td>
     <td>自定义的JSON格式Schema，支持JSON单引号和双引号模式，使用Spark-SQL时，单引号模式无需对双引号进行转义，更为方便。
     <ul>
      <li>rowkey：对于rowkey列，该列的列族名必须为"rowkey"。比如：<code>"order_id": {"cf": "rowkey","col": "order_id","type": "int"}</code></li>
      <li>数据类型：这里统一使用Spark-SQL数据类型，参考：<a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">https://spark.apache.org/docs/latest/sql-ref-datatypes.html</a></li>
    </ul>
    </td>
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

