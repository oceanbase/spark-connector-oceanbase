# OceanBase Connectors for Apache Spark

[English](README.md) | 简体中文

本仓库包含 OceanBase 的 Spark Connector。

## 功能

本仓库提供了如下 Connector：

|             Connector              |                                                             描述                                                              |                         使用文档                         |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| Spark Connector: OceanBase         | 该Connector可以通过JDBC驱动或[旁路导入](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)API将数据写入OceanBase。 | [Read & Write](docs/spark-connector-oceanbase_cn.md) |
| Spark Connector: OBKV HBase        | 该Connector通过[OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java)将数据写入OceanBase。                            | [Write](docs/spark-connector-obkv-hbase_cn.md)       |
| Spark Connector: OceanBase Catalog | OceanBase Spark Catalog让用户能够以更加简洁和一致的方式访问和操作 OceanBase 数据库。                                                                 | [Catalog](docs/spark-catalog-oceanbase_cn.md)        |

## 社区

当你需要帮助时，你可以在 [https://ask.oceanbase.com](https://ask.oceanbase.com) 上找到开发者和其他的社区伙伴。

当你发现项目缺陷时，请在 [issues](https://github.com/oceanbase/spark-connector-oceanbase/issues) 页面创建一个新的 issue。

## 许可证

更多信息见 [LICENSE](LICENSE)。
