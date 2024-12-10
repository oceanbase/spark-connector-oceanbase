/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.spark;

import com.oceanbase.spark.utils.SparkContainerTestEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class OceanBaseE2eITCase extends SparkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseE2eITCase.class);

    private static final String SINK_CONNECTOR_NAME =
            "^.*spark-connector-oceanbase-\\d+\\.\\d+_\\d+\\.\\d+-[\\d\\.]+(?:-SNAPSHOT)?\\.jar$";
    private static final String MYSQL_CONNECTOR_JAVA = "mysql-connector-java.jar";

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterAll
    public static void tearDown() {
        CONTAINER.stop();
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();

        initialize("sql/mysql/products.sql");
    }

    @AfterEach
    public void after() throws Exception {
        super.after();

        dropTables("products");
    }

    @Test
    @DisabledIfSystemProperty(
            named = "spark_version",
            matches = "^(2\\.4\\.[0-9])$",
            disabledReason =
                    "This is because the spark 2.x docker image fails to execute the spark-sql command.")
    public void testInsertValues() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY VIEW test_sink "
                                + "USING oceanbase "
                                + "OPTIONS( "
                                + "  \"url\"= \"%s\","
                                + "  \"schema-name\"=\"%s\","
                                + "  \"table-name\"=\"products\","
                                + "  \"username\"=\"%s\","
                                + "  \"password\"=\"%s\","
                                + "  \"direct-load.enabled\" = \"true\","
                                + "  \"direct-load.host\" = \"%s\","
                                + "  \"direct-load.rpc-port\" = \"%s\" "
                                + ");",
                        getJdbcUrlInContainer(),
                        getSchemaName(),
                        getUsername(),
                        getPassword(),
                        getHostInContainer(),
                        getRpcPortInContainer()));

        sqlLines.add(
                "INSERT INTO test_sink "
                        + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                        + "       (102, 'car battery', '12V car battery', 8.1),"
                        + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                        + "       (104, 'hammer', '12oz carpenter\\'s hammer', 0.75),"
                        + "       (105, 'hammer', '14oz carpenter\\'s hammer', 0.875),"
                        + "       (106, 'hammer', '16oz carpenter\\'s hammer', 1.0),"
                        + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                        + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                        + "       (109, 'spare tire', '24 inch spare tire', 22.2);");

        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA));

        List<String> expected =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.1400000000",
                        "102,car battery,12V car battery,8.1000000000",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
                        "104,hammer,12oz carpenter's hammer,0.7500000000",
                        "105,hammer,14oz carpenter's hammer,0.8750000000",
                        "106,hammer,16oz carpenter's hammer,1.0000000000",
                        "107,rocks,box of assorted rocks,5.3000000000",
                        "108,jacket,water resistent black wind breaker,0.1000000000",
                        "109,spare tire,24 inch spare tire,22.2000000000");

        waitingAndAssertTableCount("products", expected.size());

        List<String> actual = queryTable("products");
        assertEqualsInAnyOrder(expected, actual);
    }

    @Test
    @EnabledIfSystemProperty(
            named = "spark_version",
            matches = "^(2\\.4\\.[0-9])$",
            disabledReason =
                    "This is because the spark 2.x docker image fails to execute the spark-sql command.")
    public void testInsertValuesSpark2() throws Exception {
        List<String> shellLines = new ArrayList<>();

        shellLines.add(
                "val df = spark\n"
                        + "  .createDataFrame(\n"
                        + "    Seq(\n"
                        + "      (101, \"scooter\", \"Small 2-wheel scooter\", 3.14),\n"
                        + "      (102, \"car battery\", \"12V car battery\", 8.1),\n"
                        + "      (\n"
                        + "        103,\n"
                        + "        \"12-pack drill bits\",\n"
                        + "        \"12-pack of drill bits with sizes ranging from #40 to #3\",\n"
                        + "        0.8),\n"
                        + "      (104, \"hammer\", \"12oz carpenter's hammer\", 0.75),\n"
                        + "      (105, \"hammer\", \"14oz carpenter's hammer\", 0.875),\n"
                        + "      (106, \"hammer\", \"16oz carpenter's hammer\", 1.0),\n"
                        + "      (107, \"rocks\", \"box of assorted rocks\", 5.3),\n"
                        + "      (108, \"jacket\", \"water resistent black wind breaker\", 0.1),\n"
                        + "      (109, \"spare tire\", \"24 inch spare tire\", 22.2)\n"
                        + "    ))\n"
                        + "  .toDF(\"id\", \"name\", \"description\", \"weight\")");

        shellLines.add(
                String.format(
                        "import org.apache.spark.sql.SaveMode\n"
                                + "df.write\n"
                                + "  .format(\"oceanbase\")\n"
                                + "  .mode(saveMode = SaveMode.Append)\n"
                                + "  .option(\"url\", \"%s\")\n"
                                + "  .option(\"username\", \"%s\")\n"
                                + "  .option(\"password\", \"%s\")\n"
                                + "  .option(\"table-name\", \"products\")\n"
                                + "  .option(\"schema-name\", \"%s\")\n"
                                + "  .option(\"direct-load.enabled\", value = true)\n"
                                + "  .option(\"direct-load.host\", value = \"%s\")\n"
                                + "  .option(\"direct-load.rpc-port\", value = \"%s\")\n"
                                + "  .save()",
                        getJdbcUrlInContainer(),
                        getUsername(),
                        getPassword(),
                        getSchemaName(),
                        getHostInContainer(),
                        getRpcPortInContainer()));

        submitSparkShellJob(
                shellLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA));

        List<String> expected =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.1400000000",
                        "102,car battery,12V car battery,8.1000000000",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
                        "104,hammer,12oz carpenter's hammer,0.7500000000",
                        "105,hammer,14oz carpenter's hammer,0.8750000000",
                        "106,hammer,16oz carpenter's hammer,1.0000000000",
                        "107,rocks,box of assorted rocks,5.3000000000",
                        "108,jacket,water resistent black wind breaker,0.1000000000",
                        "109,spare tire,24 inch spare tire,22.2000000000");

        waitingAndAssertTableCount("products", expected.size());

        List<String> actual = queryTable("products");
        assertEqualsInAnyOrder(expected, actual);
    }
}
