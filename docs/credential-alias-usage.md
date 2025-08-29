# OceanBase Spark Connector Credential Alias Usage Guide

## Overview

To improve configuration security, OceanBase Spark Connector now supports using Hadoop Credential Provider to manage sensitive password information. Through the `alias:` prefix, users can reference passwords stored in Hadoop Credential Provider, avoiding exposing passwords in plain text in configuration files.

## Version Requirements

- **Spark**: Version 3.0 and above
- **OceanBase Spark Connector**: Version 1.3 and above

## Features

- ✅ **Hadoop Compatible**: Uses standard Hadoop Credential Provider API
- ✅ **Secure Storage**: Passwords are encrypted and stored in JCEKS files
- ✅ **Backward Compatible**: Supports plain text passwords, existing configurations require no modification
- ✅ **Multi-Environment Support**: Different environments can use different credential providers
- ✅ **Standardized**: Follows Hadoop ecosystem security best practices

## Supported Configuration Items

The following configuration items support credential alias:

### OceanBase Connector

- `password` - Database connection password

## Usage Methods

### 1. Create Credential Provider

First, create a JCEKS format credential store:

```bash
# Create credential provider
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks
```

### 2. Verify Credential

Verify that the password has been stored correctly:

```bash
# List all stored credentials
hadoop credential list -provider jceks://file/path/to/credentials.jceks

# Output example:
# database.password has been successfully stored.
```

### 3. Manage Credential

#### Update Credential

```bash
# Update existing credential
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks -force

# Or delete and recreate
hadoop credential delete database.password -provider jceks://file/path/to/credentials.jceks
hadoop credential create database.password -provider jceks://file/path/to/credentials.jceks
```

#### Delete Credential

```bash
# Delete specified credential
hadoop credential delete database.password -provider jceks://file/path/to/credentials.jceks
```

### 4. Configure Spark

#### Method 1: Configure in spark-defaults.conf

```properties
# Configure credential provider path
spark.hadoop.hadoop.security.credential.provider.path=jceks://file/path/to/credentials.jceks

# Use alias to reference password
spark.sql.catalog.ob.url=jdbc:mysql://localhost:2881/test
spark.sql.catalog.ob.username=root@sys
spark.sql.catalog.ob.password=alias:database.password
```

#### Method 2: Configure in Code

```scala
val spark = SparkSession.builder()
  .config("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
  .config("spark.sql.catalog.ob.url", "jdbc:mysql://localhost:2881/test")
  .config("spark.sql.catalog.ob.username", "root@sys")
  .config("spark.sql.catalog.ob.password", "alias:database.password")
  .config("spark.hadoop.hadoop.security.credential.provider.path",
          "jceks://file/path/to/credentials.jceks")
  .getOrCreate()

spark.sql("USE ob")
spark.sql("SELECT * FROM users").show()
```

## Advanced Configuration

### 1. Multiple Credential Providers

You can configure multiple credential providers, and the system will search in order:

```properties
spark.hadoop.hadoop.security.credential.provider.path=jceks://file/path/to/prod.jceks,jceks://file/path/to/common.jceks
```

### 2. Use Different Credential Stores for Different Environments

```bash
# Development environment
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/dev/credentials.jceks

# Test environment
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/test/credentials.jceks

# Production environment
export HADOOP_CREDENTIAL_PROVIDER_PATH=jceks://file/prod/credentials.jceks
```

### 3. Use HDFS to Store Credentials

```bash
# Create credential store stored in HDFS
hadoop credential create database.password -provider jceks://hdfs@namenode:8020/path/to/credentials.jceks

# Use in configuration
spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs@namenode:8020/path/to/credentials.jceks
```

## Security Best Practices

### 1. File Permissions

```bash
# Set secure permissions for credential file
chmod 600 /path/to/credentials.jceks
chown spark:spark /path/to/credentials.jceks

# Ensure directory permissions are also correct
chmod 700 /path/to/
```

### 2. Keystore Protection

#### Keystore Password Description

JCEKS files are protected by empty passwords by default. While this is more convenient to use, from a security perspective, it is recommended to set a strong password:

```bash
# Set keystore password (recommended)
export HADOOP_CREDSTORE_PASSWORD=your-strong-keystore-password

# Or store password in environment variable file
echo 'export HADOOP_CREDSTORE_PASSWORD=your-strong-keystore-password' >> ~/.bashrc
source ~/.bashrc

# If using default empty password, this environment variable can be omitted
```

**Security Recommendations**:
- Production environments strongly recommend setting keystore passwords
- Development/test environments can use default empty passwords
- Passwords should contain uppercase and lowercase letters, numbers, and special characters
- Regularly change keystore passwords

## Troubleshooting

### Common Errors

#### 1. Credential Provider Not Found

```
RuntimeException: No credential provider is configured or loaded.
```

**Solution**:

- Confirm the file path is correct and accessible
- Verify file permissions
- Check if `spark.hadoop.hadoop.security.credential.provider.path` is correctly set in Spark configuration

#### 2. Alias Does Not Exist

```
RuntimeException: Alias 'database.password' not found in credential provider.
```

**Solution**:
- Use `hadoop credential list` to confirm if the alias exists
- Check alias name spelling
- Confirm credential provider path is correct

#### 3. File Permission Error

```
IOException: Permission denied
```

**Solution**:
- Check file permissions: `ls -la /path/to/credentials.jceks`
- Ensure Spark process has read permissions
- Use `chmod 600` to set correct permissions
- Check directory permissions: `ls -ld /path/to/`
