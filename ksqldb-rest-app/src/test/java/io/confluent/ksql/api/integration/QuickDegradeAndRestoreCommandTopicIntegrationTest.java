package io.confluent.ksql.api.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.restore.KsqlRestoreCommandTopic;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MockSystemExit;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@Category({IntegrationTest.class})
public class QuickDegradeAndRestoreCommandTopicIntegrationTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutAutoCreateTopics()
      ).build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = KsqlTestFolder.temporaryFolder();

  private static File BACKUP_LOCATION;
  private static TestKsqlRestApp REST_APP;
  private String commandTopic;
  private Path backupFile;
  private Path propertiesFile;

  @BeforeClass
  public static void classSetUp() throws IOException {
    BACKUP_LOCATION = TMP_FOLDER.newFolder();

    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        .withProperty(KSQL_METASTORE_BACKUP_LOCATION, BACKUP_LOCATION.getPath())
        .build();
  }

  @Before
  public void setup() throws IOException {
    REST_APP.start();
    final KsqlConfig ksqlConfig = new KsqlConfig(REST_APP.getKsqlRestConfig().getKsqlConfigProperties());
    commandTopic = ReservedInternalTopics.commandTopic(ksqlConfig);
    backupFile = Files.list(BACKUP_LOCATION.toPath()).findFirst().get();
    propertiesFile = TMP_FOLDER.newFile().toPath();
    writeServerProperties(propertiesFile);
  }

  @After
  public void teardown() {
    REST_APP.stop();
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
  }

  @After
  public void teardownClass() {
    TMP_FOLDER.delete();
  }

  private static void writeServerProperties(final Path propertiesFile) throws IOException {
    final Map<String, Object> map = REST_APP.getKsqlRestConfig().getKsqlConfigProperties();

    Files.write(
        propertiesFile,
        map.keySet().stream()
            .map((key -> key + "=" + map.get(key)))
            .collect(Collectors.joining("\n"))
            .getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE
    );
  }

  @Test
  public void shouldBeInDegradeModeAfterCmdTopicDeleteAndRestart()  throws Exception {
    // Given
    TEST_HARNESS.ensureTopics("topic5");

    makeKsqlRequest("CREATE STREAM TOPIC5 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic5', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream5 AS SELECT * FROM topic5;");

    // When
    // Delete the command topic and restart
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    assertThatEventually("Topic Deleted", this::isCommandTopicDeleted, is(true));
    REST_APP.stop();
    REST_APP.start();

    // Then
    assertThatEventually("Topic Deleted", this::isCommandTopicDeleted, is(true));
    assertThatEventually("Degraded State", this::isDegradedState, is(true));
    REST_APP.stop();
    KsqlRestoreCommandTopic.mainInternal(
        new String[]{
            "--yes",
            "--config-file", propertiesFile.toString(),
            backupFile.toString()
        },
        new MockSystemExit()
    );

    // Re-load the command topic
    REST_APP.start();
    final List<String> streamsNames = showStreams();
    assertThat("Should have TOPIC5", streamsNames.contains("TOPIC5"), is(true));
    assertThat("Should have STREAM5", streamsNames.contains("STREAM5"), is(true));
    assertThatEventually("Degraded State", this::isDegradedState, is(false));
  }

  private boolean isCommandTopicDeleted() {
    return !TEST_HARNESS.topicExists(commandTopic);
  }

  private boolean isDegradedState() {
    // If in degraded state, then the following command will return a warning
    final List<KsqlEntity> response = makeKsqlRequest(
        "Show Streams;");

    final List<KsqlWarning> warnings = response.get(0).getWarnings();
    return warnings.size() > 0 &&
        (warnings.get(0).getMessage().contains(
            DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_CORRUPTED_ERROR_MESSAGE) ||
            warnings.get(0).getMessage().contains(
                DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
  }

  private List<String> showStreams() {
    return ((StreamsList)makeKsqlRequest("SHOW STREAMS;").get(0))
        .getStreams().stream().map(SourceInfo::getName).collect(Collectors.toList());
  }

  private List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }
}
