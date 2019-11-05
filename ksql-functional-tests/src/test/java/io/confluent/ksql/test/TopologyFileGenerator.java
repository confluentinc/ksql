/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.loader.ExpectedTopologiesTestLoader;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestExecutor;
import io.confluent.ksql.test.tools.TestExecutorUtil;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * This class is used to generate the topology files to ensure safe
 * upgrades of KSQL across releases.
 *
 * There are some manual steps in using this class but this should be ok as
 * we only need to create new topology files at the end of a release cycle.
 *
 * The steps to generate topology files:
 *
 * 1. Run this class by running the test {@link #manuallyGenerateTopologies} BEFORE you update the
 * pom with a new version.
 *
 * 2. This class will generate expected topology files
 * for the version specified in the pom file.  The program writes the files to
 * ksql-engine/src/test/resources/expected_topology/VERSION_NUM directory.  Where
 * VERSION_NUM is the version defined in ksql-engine/pom.xml &lt;parent&gt;&lt;version&gt; element.
 *
 */
@Ignore
public final class TopologyFileGenerator {

    /**
     * This test exists only to be able to generate topologies as part of the release process
     * It can be run manually from the IDE
     * It is deliberately excluded from the test suite
     */
    @Test
    public void manuallyGenerateTopologies() throws Exception {
        generateTopologies();
    }

    private static final StubKafkaService stubKafkaService = StubKafkaService.create();
    private static final String BASE_DIRECTORY = "src/test/resources/expected_topology/";

    static Path findBaseDir() {
        Path path = Paths.get("./ksql-functional-tests");
        if (Files.exists(path)) {
            return path.resolve(BASE_DIRECTORY);
        }
        path = Paths.get("../ksql-functional-tests");
        if (Files.exists(path)) {
            return path.resolve(BASE_DIRECTORY);
        }
        throw new RuntimeException("Failed to determine location of expected topologies directory. "
            + "App should be run with current directory set to either the root of the repo or the "
            + "root of the ksql-functional-tests module");
    }

    private static void generateTopologies() throws Exception {
        generateTopologies(findBaseDir());
    }

    static void generateTopologies(final Path base) throws Exception {
        final String formattedVersion = getFormattedVersionFromPomFile();
        final Path generatedTopologyPath = base.resolve(formattedVersion);

        System.out.println(String.format("Starting to write topology files to %s", generatedTopologyPath));

        if (!generatedTopologyPath.toFile().exists()) {
            Files.createDirectory(generatedTopologyPath);
        } else {
            System.out.println("Warning: Directory already exists, "
                + "this will re-generate topology files. dir: " + generatedTopologyPath);
        }

        writeExpectedTopologyFiles(generatedTopologyPath, getTestCases());

        System.out
            .println(String.format("Done writing topology files to %s", generatedTopologyPath));
    }

    static List<TestCase> getTestCases() {
        return QueryTranslationTest.findTestCases()
            .filter(q -> !q.expectedException().isPresent())
            .collect(Collectors.toList());
    }

    private static String getFormattedVersionFromPomFile() throws Exception {
        final File pomFile = new File("pom.xml");
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        final Document pomDoc = documentBuilder.parse(pomFile);

        final NodeList versionNodeList = pomDoc.getElementsByTagName("version");
        final String versionName = versionNodeList.item(0).getTextContent();

        return versionName.replaceAll("-SNAPSHOT?", "").replaceAll("\\.", "_");
    }

    private static void writeExpectedTopologyFiles(
        final Path topologyDir,
        final List<TestCase> testCases
    ) {
        testCases.forEach(testCase -> writeExpectedToplogyFile(topologyDir, testCase));
    }

    private static void writeExpectedToplogyFile(final Path topologyDir, final TestCase testCase) {
        try {
            final Path topologyFile = buildExpectedTopologyPath(topologyDir, testCase);

            final String topologyContent = buildExpectedTopologyContent(testCase, Optional.empty());

            Files.write(topologyFile,
                topologyContent.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Path buildExpectedTopologyPath(final Path topologyDir, final TestCase testCase) {
        return ExpectedTopologiesTestLoader.buildExpectedTopologyPath(
            testCase.getName(),
            topologyDir
        );
    }

    static String buildExpectedTopologyContent(
        final TestCase testCase,
        final Optional<Map<String, ?>> persistedConfigs
    ) {
        final KsqlConfig baseConfigs = new KsqlConfig(TestExecutor.baseConfig())
            .cloneWithPropertyOverwrite(testCase.properties());

        final KsqlConfig ksqlConfig = persistedConfigs
            .map(baseConfigs::overrideBreakingConfigsWithOriginalValues)
            .orElse(baseConfigs);

        try (final ServiceContext serviceContext = getServiceContext();
            final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)
        ) {
            final PersistentQueryMetadata queryMetadata =
                buildQuery(testCase, serviceContext, ksqlEngine, ksqlConfig);

            final Map<String, String> configsToPersist
                = new HashMap<>(ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

            // Ignore the KStreams state directory as its different every time:
            configsToPersist.remove("ksql.streams.state.dir");

            return ExpectedTopologiesTestLoader.buildExpectedTopologyContent(
                queryMetadata,
                configsToPersist
            );
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ServiceContext getServiceContext() {
        final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        return TestServiceContext.create(() -> schemaRegistryClient);
    }

    private static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
        final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
        return KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    }

    private static PersistentQueryMetadata buildQuery(
        final TestCase testCase,
        final ServiceContext serviceContext,
        final KsqlEngine ksqlEngine,
        final KsqlConfig ksqlConfig
    ) {
        final List<PersistentQueryMetadata> queries = TestExecutorUtil
            .buildQueries(testCase, serviceContext, ksqlEngine, ksqlConfig, stubKafkaService);

        final MetaStore metaStore = ksqlEngine.getMetaStore();
        for (QueryMetadata queryMetadata: queries) {
            final PersistentQueryMetadata persistentQueryMetadata
                = (PersistentQueryMetadata) queryMetadata;
            final String sinkKafkaTopicName = metaStore
                .getSource(persistentQueryMetadata.getSinkName())
                .getKafkaTopicName();

            final SerdeSupplier<?> keySerdes = SerdeUtil.getKeySerdeSupplier(
                persistentQueryMetadata.getResultTopic().getKeyFormat(),
                queryMetadata::getLogicalSchema
            );

            final SerdeSupplier<?> valueSerdes = SerdeUtil.getSerdeSupplier(
                persistentQueryMetadata.getResultTopic().getValueFormat().getFormat(),
                queryMetadata::getLogicalSchema
            );

            final Topic sinkTopic = new Topic(
                sinkKafkaTopicName,
                Optional.empty(),
                keySerdes,
                valueSerdes,
                1,
                1,
                Optional.empty()
            );

            stubKafkaService.createTopic(sinkTopic);
        }

        assertThat("test did not generate any queries.", queries.isEmpty(), is(false));
        return queries.get(queries.size() - 1);
    }
}
