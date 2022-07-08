/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.COMMAND_RUNNER_CHECK_NAME;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Tests covering integration tests for backup/restore the command topic.
 */
@Category({IntegrationTest.class})
public class BackupRollbackIntegrationTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

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
  private KsqlRestConfig ksqlRestConfig;
  private KsqlConfig ksqlConfig;

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
    ksqlRestConfig = REST_APP.getKsqlRestConfig();
    ksqlConfig = new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties());
    commandTopic = ReservedInternalTopics.commandTopic(ksqlConfig);
    backupFile = Files.list(BACKUP_LOCATION.toPath()).findFirst().get();
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

  @Test
  public void shouldEnterDegradedStateWithBackupEnabled() {
    // Given
    TEST_HARNESS.ensureTopics("topic1", "topic2");

    makeKsqlRequest("CREATE STREAM TOPIC1 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic1', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM TOPIC2 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic2', VALUE_FORMAT='JSON');");
    makeKsqlRequest("CREATE STREAM stream1 AS SELECT * FROM topic1;");
    makeKsqlRequest("CREATE STREAM stream2 AS SELECT * FROM topic2;");
    
    // When
    final CommandId commandId = new CommandId("TOPIC", "entity", "CREATE");
    final Command command = new Command(
        "statement",
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        Optional.of(Command.VERSION + 1),
        Command.VERSION + 1);

    produceToCommandTopic(commandId, command);

    // Server should enter degraded state due to incompatible command
    assertThatEventually("Degraded State", this::isDegradedState, is(true));
    try {
      List<String> allLines = Files.readAllLines(backupFile);
      assertThat("All commands backed up from command topic", allLines.size() == 5);
      assertThat("Incompatible command in backup", allLines.get(allLines.size() - 1).contains("\"version\":" + (Command.VERSION + 1)));
    } catch (IOException e) {
      e.printStackTrace();
    }
  
    

    // Re-load the command topic
    REST_APP.stop();
    REST_APP.start();

    // Then
    final List<String> streamsNames = showStreams();
    assertThat("Should have TOPIC1", streamsNames.contains("TOPIC1"), is(true));
    assertThat("Should have TOPIC2", streamsNames.contains("TOPIC2"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Should have STREAM2", streamsNames.contains("STREAM2"), is(true));
    assertThat("Server should be in degraded state", isDegradedState(), is(true));
  }

  private boolean isDegradedState() {
    // If in degraded state, then the following command will return a warning
    final List<KsqlEntity> response = makeKsqlRequest("Show Streams;");

    final List<KsqlWarning> warnings = response.get(0).getWarnings();

    final HealthCheckResponse res = RestIntegrationTestUtil.checkServerHealth(REST_APP);
    
    return !res.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy() &&(warnings.size() > 0 &&
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
  
  private void produceToCommandTopic(
      final CommandId commandId,
      final Command command
  ) {
    final Map<String, Object> kafkaProducerProperties = REST_APP.getKsqlRestConfig().getCommandProducerProperties();
    kafkaProducerProperties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
    );
    kafkaProducerProperties.put(
        ProducerConfig.ACKS_CONFIG,
        "all"
    );
    final KafkaProducer<CommandId, Command> producer = new KafkaProducer<>(
        kafkaProducerProperties,
        InternalTopicSerdes.serializer(),
        InternalTopicSerdes.serializer()
    );

    try {
      producer.initTransactions();
    } catch (final Exception e) {
      assertThat("Fail due to not initializing transaction properly", false);
    }

    try {
      producer.beginTransaction();
      assertThatEventually("CommandRunner up to date", () -> showStreams().size(), is(4));
      producer.send(new ProducerRecord<>(
          ReservedInternalTopics.commandTopic(ksqlConfig),
          0,
          commandId,
          command));
      producer.commitTransaction();
    } catch (final Exception e) {
      producer.abortTransaction();
      assertThat("Failed to produce to command topic properly", false);
    } finally {
      producer.close();
    }
  }
}
