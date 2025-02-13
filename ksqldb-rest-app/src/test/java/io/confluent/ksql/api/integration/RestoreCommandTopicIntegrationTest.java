/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static java.lang.Thread.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.BackupReplayFile;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.rest.server.restore.KsqlRestoreCommandTopic;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

/**
 * Tests covering integration tests for backup/restore the command topic.
 */
@Category({IntegrationTest.class})
public class RestoreCommandTopicIntegrationTest {
  private static final MockSystemExit mockSystem = new MockSystemExit();
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = KsqlTestFolder.temporaryFolder();

  private static File BACKUP_LOCATION;
  private static File STATE_DIR;
  private static TestKsqlRestApp REST_APP;
  private String commandTopic;
  private Path backupFile;
  private Path propertiesFile;

  @BeforeClass
  public static void classSetUp() throws IOException {
    BACKUP_LOCATION = TMP_FOLDER.newFolder();
    STATE_DIR = TMP_FOLDER.newFolder();

    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        .withProperty(KSQL_METASTORE_BACKUP_LOCATION, BACKUP_LOCATION.getPath())
        .withProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR.getAbsolutePath())
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
  public void shouldBackupAndRestoreCommandTopic() throws Exception {
    // Given
    TEST_HARNESS.ensureTopics("topic1", "topic2");

    makeKsqlRequest("CREATE STREAM TOPIC1 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic1', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM TOPIC2 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic2', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream1 AS SELECT * FROM topic1;");
    makeKsqlRequest("CREATE STREAM stream2 AS SELECT * FROM topic2;");

    // When

    // Delete the command topic and check the server is in degraded state
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    assertThatEventually("Degraded State", this::isDegradedState, is(true));

    // Restore the command topic
    KsqlRestoreCommandTopic.mainInternal(
        new String[]{
            "--yes",
            "--config-file", propertiesFile.toString(),
            backupFile.toString()
        },
        mockSystem
    );

    // Re-load the command topic
    REST_APP.stop();
    REST_APP.start();

    // Then
    final List<String> streamsNames = showStreams();
    assertThat("Should have TOPIC1", streamsNames.contains("TOPIC1"), is(true));
    assertThat("Should have TOPIC2", streamsNames.contains("TOPIC2"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Should have STREAM2", streamsNames.contains("STREAM2"), is(true));
    assertThat("Server should NOT be in degraded state", isDegradedState(), is(false));
  }

  @Test
  public void shouldSkipIncompatibleCommands() throws Exception {
    // Given
    TEST_HARNESS.ensureTopics("topic3", "topic4");

    makeKsqlRequest("CREATE STREAM TOPIC3 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic3', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM TOPIC4 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic4', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream3 AS SELECT * FROM topic3;");

    final CommandId commandId = new CommandId("TOPIC", "entity", "CREATE");
    final Command command = new Command(
        "statement",
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        Optional.of(Command.VERSION + 1),
        Command.VERSION + 1);

    writeToBackupFile(commandId, command, backupFile);

    // Delete the command topic again and restore with skip flag
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    REST_APP.stop();
    KsqlRestoreCommandTopic.mainInternal(
        new String[]{
            "--yes",
            "-s",
            "--config-file", propertiesFile.toString(),
            backupFile.toString()
        },
        mockSystem
    );

    // Re-load the command topic
    REST_APP.start();
    final List<String> streamsNames = showStreams();
    assertThat("Should have TOPIC3", streamsNames.contains("TOPIC3"), is(true));
    assertThat("Should have TOPIC4", streamsNames.contains("TOPIC4"), is(true));
    assertThat("Should have STREAM3", streamsNames.contains("STREAM3"), is(true));
    assertThat("Server should not be in degraded state", isDegradedState(), is(false));
  }

  @Test
  public void shouldSkipIncompatibleCommandsWithExecutionPlan() throws Exception {
    // Given
    TEST_HARNESS.ensureTopics("topic3", "topic4");

    makeKsqlRequest("CREATE STREAM TOPIC3 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic3', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM TOPIC4 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic4', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream3 AS SELECT * FROM topic3;");



    final CommandId commandId = new CommandId("TOPIC", "entity", "CREATE");
    final String command = "{\"statement\":\"CREATE STREAM \"," 
        + "\"streamsProperties\":{},"
        + "\"originalProperties\":{},"
        + "\"plan\":{"
        + "\"@type\":\"ksqlPlanV1\","
        + "\"statementText\":\"CREATE STREAM \","
        + "\"ddlCommand\":{},"
        + "\"queryPlan\":{"
        + "\"sources\":[\"RIDERLOCATIONSFILTERED\"],"
        + "\"sink\":\"0_29_0_STREAM\","
        + "\"physicalPlan\":{"
        + "\"@type\":\"streamSinkV1\","
        + "\"properties\":{\"queryContext\":\"0_29_0_STREAM\"},"
        + "\"source\":{"
        + "\"@type\":\"streamSelectV1\","
        + "\"properties\":{\"queryContext\":\"Project\"},"
        + "\"source\":{},"
        + "\"keyColumnNames\":[],"
        + "\"selectedKeys\":null,"
        + "\"selectExpressions\":[\"PROFILEID AS PROFILEID\",\"LATITUDE AS LATITUDE\",\"LONGITUDE AS LONGITUDE\"]},"
        + "\"formats\":{},"
        + "\"topicName\":\"0_29_0_STREAM\",\"timestampColumn\":null},"
        + "\"queryId\":\"CSAS_0_29_0_STREAM_5\","
        + "\"runtimeId\":null}},"
        + "\"version\":"
        + Command.VERSION + 1
        + "}";

    writeStringToBackupFile(commandId, command, backupFile);

    // Delete the command topic again and restore with skip flag
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    REST_APP.stop();
    KsqlRestoreCommandTopic.mainInternal(
        new String[]{
            "--yes",
            "-s",
            "--config-file", propertiesFile.toString(),
            backupFile.toString()
        },
        mockSystem
    );

    // Re-load the command topic
    REST_APP.start();
    final List<String> streamsNames = showStreams();
    assertThat("Should have 3 things", streamsNames.size(), is(3));
    assertThat("Should have TOPIC3", streamsNames.contains("TOPIC3"), is(true));
    assertThat("Should have TOPIC4", streamsNames.contains("TOPIC4"), is(true));
    assertThat("Should have STREAM3", streamsNames.contains("STREAM3"), is(true));
    assertThat("Server should not be in degraded state", isDegradedState(), is(false));
  }

  @Test
  public void shouldHandleIncompatibleCommandsWithNoQueryPlan() throws Exception {
    // Given
    TEST_HARNESS.ensureTopics("topic3", "topic4");

    makeKsqlRequest("CREATE STREAM TOPIC3 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic3', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM TOPIC4 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic4', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream3 AS SELECT * FROM topic3;");

    final CommandId commandId = new CommandId("TOPIC", "entity", "CREATE");
    final String command = "{\"statement\":\"CREATE STREAM \","
        + "\"streamsProperties\":{},"
        + "\"originalProperties\":{},"
        + "\"plan\":{"
        + "\"@type\":\"ksqlPlanV1\","
        + "\"statementText\":\"CREATE STREAM \","
        + "\"ddlCommand\":{},"
        + "\"queryPlan\": null},"
        + "\"version\":"
        + Command.VERSION + 1
        + "}";

    writeStringToBackupFile(commandId, command, backupFile);

    // Delete the command topic again and restore with skip flag
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    REST_APP.stop();
    KsqlRestoreCommandTopic.mainInternal(
        new String[]{
            "--yes",
            "-s",
            "--config-file", propertiesFile.toString(),
            backupFile.toString()
        },
        mockSystem
    );

    // Re-load the command topic
    REST_APP.start();
    final List<String> streamsNames = showStreams();
    assertThat("Should have 3 things", streamsNames.size(), is(3));
    assertThat("Should have TOPIC3", streamsNames.contains("TOPIC3"), is(true));
    assertThat("Should have TOPIC4", streamsNames.contains("TOPIC4"), is(true));
    assertThat("Should have STREAM3", streamsNames.contains("STREAM3"), is(true));
    assertThat("Server should not be in degraded state", isDegradedState(), is(false));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  @Test
  public void shouldCleanUpLeftoverStateStores() throws InterruptedException {
    // Given:
    File fakeStateStore = new File(STATE_DIR.getAbsolutePath() + "/fakeStateStore");
    if (!fakeStateStore.exists()){
      fakeStateStore.mkdirs();
    }
    makeKsqlRequest("CREATE STREAM new_stream (ID INT, price int) "
            + "WITH (KAFKA_TOPIC='temp_top', partitions=3, VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE TABLE new_stream_3 AS SELECT id, sum(price) FROM new_stream group by ID;");
    File realStateStore = new File(STATE_DIR.getAbsolutePath() + "/_confluent-ksql-default_query_CTAS_NEW_STREAM_3_1");

    assertTrue(STATE_DIR.exists());
    assertTrue(fakeStateStore.exists());
    assertTrue(realStateStore.exists());

    // When:
    REST_APP.stop();
    REST_APP.start();
    sleep(3000);

    // Then:
    assertFalse(fakeStateStore.exists());
    assertTrue(realStateStore.exists());
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

  private static void writeToBackupFile(
      final CommandId commandId,
      final Command command,
      final Path backUpFileLocation
  ) throws IOException {
    BackupReplayFile.writable(new File(String.valueOf(backUpFileLocation)))
        .write(new ConsumerRecord<>(
            "",
            0,
            0L,
            InternalTopicSerdes.serializer().serialize("", commandId),
            InternalTopicSerdes.serializer().serialize("", command))
        );
  }
  private static void writeStringToBackupFile(
      final CommandId commandId,
      final String command,
      final Path backUpFileLocation
  ) throws IOException {
    BackupReplayFile.writable(new File(String.valueOf(backUpFileLocation)))
        .write(new ConsumerRecord<>(
            "",
            0,
            0L,
            InternalTopicSerdes.serializer().serialize("", commandId),
            command.getBytes(StandardCharsets.UTF_8))
        );
  }
}
