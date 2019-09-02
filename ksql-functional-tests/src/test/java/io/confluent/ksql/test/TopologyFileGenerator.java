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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
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
import io.confluent.ksql.test.tools.FakeKafkaService;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
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
 * 1. Run this class BEFORE you update the pom with a new version.
 *
 * 2. This class will generate expected topology files
 * for the version specified in the pom file.  The program writes the files to
 * ksql-engine/src/test/resources/expected_topology/VERSION_NUM directory.  Where
 * VERSION_NUM is the version defined in ksql-engine/pom.xml &lt;parent&gt;&lt;version&gt; element.
 *
 */
@Category(IntegrationTest.class)
public final class TopologyFileGenerator {

    private static final FakeKafkaService fakeKafkaService = FakeKafkaService.create();
    private static final String BASE_DIRECTORY = "src/test/resources/expected_topology/";

    private TopologyFileGenerator() {
    }

    public static void main(final String[] args) throws Exception {
        generateTopologies(findBaseDir());
    }

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

    private static List<TestCase> getTestCases() {
        return QueryTranslationTest.findTestCases()
            .filter(q -> !q.isAnyExceptionExpected())
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
        final ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();

        testCases.forEach(testCase -> {
            final KsqlConfig ksqlConfig = new KsqlConfig(baseConfig())
                .cloneWithPropertyOverwrite(testCase.properties());

            try (final ServiceContext serviceContext = getServiceContext();
                final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)) {

                final PersistentQueryMetadata queryMetadata =
                    buildQuery(testCase, serviceContext, ksqlEngine, ksqlConfig);
                final Map<String, String> configsToPersist
                    = new HashMap<>(ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

                // Ignore the KStreams state directory as its different every time:
                configsToPersist.remove("ksql.streams.state.dir");

                ExpectedTopologiesTestLoader.writeExpectedTopologyFile(
                    testCase.name,
                    queryMetadata,
                    configsToPersist,
                    objectWriter,
                    topologyDir);
            }
        });
    }

    private static Map<String, Object> baseConfig() {
        return ImmutableMap.<String, Object>builder()
            .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
            .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
            .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
            .put(StreamsConfig.APPLICATION_ID_CONFIG, "some.ksql.service.id")
            .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
            .put(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
            .put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all")
            .build();
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
        testCase.initializeTopics(
            serviceContext.getTopicClient(),
            fakeKafkaService,
            serviceContext.getSchemaRegistryClient());

        final String sql = testCase.statements().stream()
            .collect(Collectors.joining(System.lineSeparator()));

        final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
            ksqlEngine,
            sql,
            ksqlConfig,
            new HashMap<>(),
            Optional.of(serviceContext.getSchemaRegistryClient())
        );

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

            fakeKafkaService.createTopic(sinkTopic);
        }

        assertThat("test did not generate any queries.", queries.isEmpty(), is(false));
        return (PersistentQueryMetadata) queries.get(queries.size() - 1);
    }
}
