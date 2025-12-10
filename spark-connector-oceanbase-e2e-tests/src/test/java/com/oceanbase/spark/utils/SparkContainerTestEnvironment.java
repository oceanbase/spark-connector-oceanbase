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

package com.oceanbase.spark.utils;

import com.oceanbase.spark.OceanBaseMySQLTestBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

public abstract class SparkContainerTestEnvironment extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkContainerTestEnvironment.class);

    private static final String SPARK_VERSION = System.getProperty("spark_version");
    private static final String MODULE_DIRECTORY = System.getProperty("moduleDir", "");

    private static final String INTER_CONTAINER_JM_ALIAS = "spark";

    protected String getSparkDockerImageTag() {
        return String.format("apache/spark:%s", SPARK_VERSION);
    }

    @TempDir public java.nio.file.Path temporaryFolder;

    public GenericContainer<?> sparkContainer;

    @SuppressWarnings("resource")
    @BeforeEach
    public void before() throws Exception {
        LOG.info("Starting Spark containers...");
        sparkContainer =
                new GenericContainer<>(getSparkDockerImageTag())
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withCommand("bash", "-lc", "sleep infinity");

        Startables.deepStart(Stream.of(sparkContainer)).join();
        // Ensure a generic jars directory exists regardless of image layout
        sparkContainer.execInContainer("bash", "-lc", "mkdir -p /tmp/jars");
        LOG.info("Spark containers started");
    }

    @AfterEach
    public void after() throws Exception {
        if (sparkContainer != null) {
            sparkContainer.stop();
        }
    }

    public String getJdbcUrlInContainer() {
        return "jdbc:mysql://"
                + getHostInContainer()
                + ":"
                + getPortInContainer()
                + "/"
                + getSchemaName()
                + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }

    public String getHostInContainer() {
        return getOBServerIP();
    }

    public int getPortInContainer() {
        return 2881;
    }

    public int getRpcPortInContainer() {
        return 2882;
    }

    /**
     * Searches for a resource file matching the given regex in the given directory. This method is
     * primarily intended to be used for the initialization of static {@link Path} fields for
     * resource file(i.e. jar, config file). if resolvePaths is empty, this method will search file
     * under the modules {@code target} directory. if resolvePaths is not empty, this method will
     * search file under resolvePaths of current project.
     *
     * @param resourceNameRegex regex pattern to match against
     * @param resolvePaths an array of resolve paths of current project
     * @return Path pointing to the matching file
     * @throws RuntimeException if none or multiple resource files could be found
     */
    public static Path getResource(final String resourceNameRegex, String... resolvePaths) {
        Path path = Paths.get(MODULE_DIRECTORY).toAbsolutePath();

        if (resolvePaths != null && resolvePaths.length > 0) {
            path = path.getParent().getParent();
            for (String resolvePath : resolvePaths) {
                path = path.resolve(resolvePath);
            }
        }

        try (Stream<Path> dependencyResources = Files.walk(path)) {
            // Extract version prefix (e.g., "3.1" from "v3.1.3" or "3.1.3")
            final String versionPrefix =
                    SPARK_VERSION.startsWith("v")
                            ? SPARK_VERSION.substring(1, Math.min(4, SPARK_VERSION.length()))
                            : SPARK_VERSION.substring(0, Math.min(3, SPARK_VERSION.length()));

            final List<Path> matchingResources =
                    dependencyResources
                            .filter(
                                    file ->
                                            file.toAbsolutePath().toString().contains(versionPrefix)
                                                    || file.toAbsolutePath()
                                                            .toString()
                                                            .contains("mysql"))
                            .filter(
                                    jar ->
                                            Pattern.compile(resourceNameRegex)
                                                    .matcher(jar.toAbsolutePath().toString())
                                                    .find())
                            .collect(Collectors.toList());
            switch (matchingResources.size()) {
                case 0:
                    throw new RuntimeException(
                            new FileNotFoundException(
                                    String.format(
                                            "No resource file could be found that matches the pattern %s. "
                                                    + "This could mean that the test module must be rebuilt via maven.",
                                            resourceNameRegex)));
                case 1:
                    return matchingResources.get(0);
                default:
                    throw new RuntimeException(
                            new IOException(
                                    String.format(
                                            "Multiple resource files were found matching the pattern %s. Matches=%s",
                                            resourceNameRegex, matchingResources)));
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Could not search for resource resource files.", ioe);
        }
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(List<String> sqlLines, Path... jars)
            throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        Path script = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toPath();
        Files.write(script, sqlLines);
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");
        commands.add("--jars");
        String jarStr =
                Arrays.stream(jars)
                        .map(
                                jar ->
                                        copyAndGetContainerPath(
                                                sparkContainer, jar.toAbsolutePath().toString()))
                        .collect(Collectors.joining(","));
        commands.add(jarStr);
        commands.add("-f /tmp/script.sql");

        String command = String.format("/opt/spark/bin/spark-sql %s", String.join(" ", commands));
        LOG.info(command);
        Container.ExecResult execResult = sparkContainer.execInContainer("bash", "-c", command);
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    public void submitSparkShellJob(List<String> shellLines, Path... jars)
            throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        Path script = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toPath();
        Files.write(script, shellLines);
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.scala");
        commands.add("--jars");
        String jarStr =
                Arrays.stream(jars)
                        .map(
                                jar ->
                                        copyAndGetContainerPath(
                                                sparkContainer, jar.toAbsolutePath().toString()))
                        .collect(Collectors.joining(","));
        commands.add(jarStr);
        commands.add("-i /tmp/script.scala");

        String command =
                String.format(
                        "timeout 2m /opt/spark/bin/spark-shell %s", String.join(" ", commands));
        LOG.info(command);
        Container.ExecResult execResult = sparkContainer.execInContainer("bash", "-c", command);
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
    }

    private String copyAndGetContainerPath(GenericContainer<?> container, String filePath) {
        Path path = Paths.get(filePath);
        String containerPath = "/tmp/jars/" + path.getFileName();
        container.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
        return containerPath;
    }
}
