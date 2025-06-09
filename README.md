# OceanBase Connectors for Apache Spark

This repository contains the OceanBase connectors for Apache Spark.

English | [简体中文](README_CN.md)

## Features

This repository contains connectors as following:

|             Connector              |                                                                             Description                                                                             |                         Document                          |                         Note                         |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|------------------------------------------------------|
| Spark Connector: OceanBase Catalog | OceanBase Spark Catalog allows users to access and operate the OceanBase database in a more concise and consistent way.                                             | [Catalog & Read & Write](docs/spark-catalog-oceanbase.md) | Supports Spark 3.x and above                         |
| Spark Connector: OBKV HBase        | This Connector uses the [OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java) to write data to OceanBase.                                           | [Write](docs/spark-connector-obkv-hbase.md)               |                                                      |
| Spark Connector: OceanBase         | This Connector uses the JDBC driver or the [direct load](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001375568) API to write data to OceanBase. | [Read & Write](docs/spark-connector-oceanbase.md)         | Gradually deprecated, recommended only for Spark 2.x |

## Community

Don’t hesitate to ask!

Contact the developers and community at [https://ask.oceanbase.com](https://ask.oceanbase.com) if you need any help.

[Open an issue](https://github.com/oceanbase/spark-connector-oceanbase/issues) if you found a bug.

## Licensing

See [LICENSE](LICENSE) for more information.
