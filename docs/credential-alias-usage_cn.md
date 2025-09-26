# OceanBase Spark Connector Credential Alias 使用指南

## 概述

为了提高配置安全性，OceanBase Spark Connector 现在支持使用 Hadoop Credential Provider 来管理敏感的密码信息。通过 `alias:` 前缀，用户可以引用存储在 Hadoop Credential Provider 中的密码，避免在配置文件中明文暴露密码。

## 版本要求

- **Spark**: 3.0 及以上版本
- **OceanBase Spark Connector**: 1.3 及以上版本

## 功能特性

- ✅ **Hadoop 兼容**: 使用标准的 Hadoop Credential Provider API
- ✅ **安全存储**: 密码加密存储在 JCEKS 文件中
- ✅ **向下兼容**: 支持明文密码，现有配置无需修改
- ✅ **多环境支持**: 不同环境可使用不同的 credential provider
- ✅ **标准化**: 遵循 Hadoop 生态系统的安全最佳实践

## 支持的配置项

以下配置项支持 credential alias：

### OceanBase Connector

- `password` - 数据库连接密码

## 使用方法

### 1. 创建 Credential Provider

首先创建一个 JCEKS 格式的 credential store：

```bash
# 创建 credential provider
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks
```

### 2. 验证 Credential

验证密码已正确存储：

```bash
# 列出所有存储的 credential
hadoop credential list -provider jceks://file/path/to/credentials.jceks

# 输出示例：
# database.password has been successfully stored.
```

### 3. 管理 Credential

#### 更新 Credential

```bash
# 更新已存在的 credential
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks -force

# 或者删除后重新创建
hadoop credential delete database.password -provider jceks://file/path/to/credentials.jceks
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks
```

#### 删除 Credential

```bash
# 删除指定的 credential
hadoop credential delete database.password -provider jceks://file/path/to/credentials.jceks
```

### 4. 配置 Spark

#### 方式 1: 在 spark-defaults.conf 中配置

```properties
# 配置 credential provider 路径
spark.hadoop.hadoop.security.credential.provider.path=jceks://file/path/to/credentials.jceks

# 使用 alias 引用密码
spark.oceanbase.url=jdbc:mysql://localhost:2881/test
spark.oceanbase.username=root@sys
spark.oceanbase.password=alias:database.password
spark.oceanbase.schema-name=test
```

### 方式 2: 在代码中中配置

```scala
val spark = SparkSession.builder()
  .config("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
  .config("spark.sql.catalog.ob.url", "jdbc:mysql://localhost:2881/test")
  .config("spark.sql.catalog.ob.username", "root@sys")
  .config("spark.sql.catalog.ob.password", "alias:database.password")
  .config("spark.sql.catalog.ob.schema-name", "test")
  .config("spark.hadoop.hadoop.security.credential.provider.path",
          "jceks://file/path/to/credentials.jceks")
  .getOrCreate()

spark.sql("USE ob")
spark.sql("SELECT * FROM users").show()
```

## 高级配置

### 1. 多个 Credential Provider

可以配置多个 credential provider，系统会按顺序查找：

```properties
spark.hadoop.hadoop.security.credential.provider.path=jceks://file/path/to/prod.jceks,jceks://file/path/to/common.jceks
```

### 2. 不同环境使用不同的 Credential Store

```bash
# 开发环境
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/dev/credentials.jceks

# 测试环境
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/test/credentials.jceks

# 生产环境
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/prod/credentials.jceks
```

### 3. 使用 HDFS 存储 Credential

```bash
# 创建存储在 HDFS 的 credential store
hadoop credential create database.password -provider jceks://hdfs@namenode:8020/path/to/credentials.jceks

# 在配置中使用
spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs@namenode:8020/path/to/credentials.jceks
```

## 安全最佳实践

### 1. 文件权限

```bash
# 设置 credential 文件的安全权限
chmod 600 /path/to/credentials.jceks
chown spark:spark /path/to/credentials.jceks

# 确保目录权限也正确
chmod 700 /path/to/
```

### 2. 密钥存储保护

#### Keystore 密码说明

JCEKS 文件默认使用空密码保护。虽然这样使用起来更方便，但从安全角度考虑，建议设置一个强密码：

```bash
# 设置 keystore 密码（推荐）
export HADOOP_CREDSTORE_PASSWORD=your-strong-keystore-password

# 或者将密码存储在环境变量文件中
echo 'export HADOOP_CREDSTORE_PASSWORD=your-strong-keystore-password' >> ~/.bashrc
source ~/.bashrc

# 如果使用默认的空密码，可以不设置此环境变量
```

**安全建议**：
- 生产环境强烈建议设置 keystore 密码
- 开发/测试环境可以使用默认空密码
- 密码应包含大小写字母、数字和特殊字符
- 定期更换 keystore 密码

## 故障排除

### 常见错误

#### 1. Credential Provider 未找到

```
RuntimeException: No credential provider is configured or loaded.
```

**解决方案**：

- 确认文件路径正确且可访问
- 验证文件权限
- 检查 Spark 配置中是否正确设置了 `spark.hadoop.hadoop.security.credential.provider.path`

#### 2. Alias 不存在

```
RuntimeException: Alias 'database.password' not found in credential provider.
```

**解决方案**：
- 使用 `hadoop credential list` 确认 alias 是否存在
- 检查 alias 名称拼写
- 确认 credential provider 路径正确

#### 3. 文件权限错误

```
IOException: Permission denied
```

**解决方案**：
- 检查文件权限：`ls -la /path/to/credentials.jceks`
- 确保 Spark 进程有读取权限
- 使用 `chmod 600` 设置正确权限
- 检查目录权限：`ls -ld /path/to/`

