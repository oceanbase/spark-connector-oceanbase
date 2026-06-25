# Spark 3 DataFrame V2 Reader Change Summary

## What Changed

- Spark 3 `spark.read.format("oceanbase")` now exposes `OceanBaseSparkDataSource` as a Spark DataSource V2 `TableProvider` while keeping the existing `JdbcRelationProvider` implementation.
- Default DataFrame table reads now resolve to the existing Catalog table implementation:
  - `OceanBaseSparkDataSource.getTable`
  - `OceanBaseSourceUtils.resolveTable`
  - `OceanBaseTable`
  - `OBJdbcScanBuilder`
  - `OBJdbcReader`
- This makes the DataFrame API reuse the Catalog reader path for adaptive partition planning, predicate pushdown, column pruning, aggregate/limit/top-N pushdown, query hints, and MySQL/Oracle dialect-specific reader behavior.
- A small shared resolver, `OceanBaseSourceUtils`, centralizes:
  - `CaseInsensitiveStringMap` options to `OceanBaseConfig`
  - compatible mode to `OceanBaseDialect`
  - `schema-name`/URL database/table name to catalog-style `Identifier`
  - quoted `dbTable`
  - schema resolution through `OceanBaseCatalog.resolveTable`

## Compatibility Behavior

- `query` option keeps the old V1 JDBC read behavior because arbitrary subqueries cannot be represented as a Catalog table.
- `enable_v2_reader=false` forces the V1 JDBC read behavior.
- The fallback is surfaced through a V2 wrapper table:
  - `OceanBaseLegacyTable`
  - `OceanBaseLegacyScanBuilder`
  - `OceanBaseLegacyScan`
- The wrapper delegates schema, partitioning, filter handling, and scan execution to the existing `OceanBaseJDBCRelation` path.
- Spark 2.4 code was not changed.
- No new Spark 3.x base module was added; Spark 3.1, 3.2, 3.3, 3.4, and 3.5 continue to compile from the existing inheritance/module layout.
- Write code remains in the existing `createRelation` path. Because the provider now also implements `TableProvider`, write behavior should be covered by the existing write integration tests once Docker/Testcontainers is available.

## Tests Updated

- `testDataFrameRead` now verifies that default DataFrame API reads use `OceanBaseTable`.
- `testSqlReadWithQuery` now verifies that `query` reads use `OceanBaseLegacyTable`.
- Added `testDataFrameReadWithV2ReaderDisabled` to verify that `enable_v2_reader=false` uses `OceanBaseLegacyTable` and preserves result compatibility.
- Added `testDataFrameReadMatchesCatalogV2ReaderBehavior` to compare Catalog reads and DataFrame API reads in the same Spark session for:
  - filter pushdown
  - column pruning
  - limit pushdown
  - order by plus limit top-N pushdown
  - MySQL aggregate pushdown
- Added `testDataFrameV2ReadHonorsLegacyBatchReaderOption` to verify that DataFrame reads still honor `enable_legacy_batch_reader=true` while resolving through `OceanBaseTable`.
- Added `OBJdbcBatchScan.description` details so integration tests can assert the pushed scan state from Spark physical plans:
  - required columns
  - pushed filters
  - pushed limit
  - pushed top-N orders
  - pushed aggregates and group-by columns

## Verification

All successful verification commands were run with Java 8:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home
```

- Passed base validation:

```bash
'/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn' \
  -pl spark-connector-oceanbase-common,spark-connector-oceanbase/spark-connector-oceanbase-base \
  -am -DskipTests validate
```

- Passed Spark 3.1 through 3.5 compilation:

```bash
'/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn' \
  -pl spark-connector-oceanbase-common,spark-connector-oceanbase/spark-connector-oceanbase-base,\
spark-connector-oceanbase/spark-connector-oceanbase-3.1,\
spark-connector-oceanbase/spark-connector-oceanbase-3.2,\
spark-connector-oceanbase/spark-connector-oceanbase-3.3,\
spark-connector-oceanbase/spark-connector-oceanbase-3.4,\
spark-connector-oceanbase/spark-connector-oceanbase-3.5 \
  -am -DskipTests -Dspotless.check.skip=true -Drat.skip=true compile
```

- Passed base test compilation:

```bash
'/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn' \
  -pl spark-connector-oceanbase-common,spark-connector-oceanbase/spark-connector-oceanbase-base \
  -am -DskipTests -Dspotless.check.skip=true -Drat.skip=true test-compile
```

- Attempted targeted MySQL integration test:

```bash
'/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn' \
  -pl spark-connector-oceanbase-common,spark-connector-oceanbase/spark-connector-oceanbase-base \
  -am -Dspotless.check.skip=true -Drat.skip=true -DfailIfNoTests=false \
  -Dtest=OceanBaseMySQLConnectorITCase test
```

The targeted integration test compiled the test sources and started `OceanBaseMySQLConnectorITCase`, but runtime execution was blocked by the local environment:

```text
Could not find a valid Docker environment
Root cause NoSuchFileException (/var/run/docker.sock)
```

This means the Docker/Testcontainers-backed e2e assertions still need to be rerun in an environment with Docker available.

## GitHub Actions Plan

Local Docker is unavailable, so the branch should be pushed to GitHub and the repository workflow should run the Docker/Testcontainers-backed checks there. The relevant branch is `df_v2`.

## GitHub Actions Status

- Pushed branch: `wary/spark-connector-oceanbase:df_v2`
- Fork PR: <https://github.com/wary/spark-connector-oceanbase/pull/1>
- Upstream PR: <https://github.com/oceanbase/spark-connector-oceanbase/pull/111>
- Upstream CI run: <https://github.com/oceanbase/spark-connector-oceanbase/actions/runs/28181760003>

The upstream PR triggered the CI workflow, but GitHub completed the run with `action_required` before starting any jobs. The run contains no job logs yet. The PR currently reports:

```text
license/cla: pending
CI: action_required
```

This means the Docker/Testcontainers-backed e2e matrix has not executed yet. A repository maintainer needs to approve the fork PR workflow run, and the CLA check needs to be completed, before CI can provide actionable test logs.
