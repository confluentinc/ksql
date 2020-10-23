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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.restore.KsqlRestoreCommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

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

import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


/**
 * Tests covering integration tests for backup/restore the command topic.
 */
@Category({IntegrationTest.class})
public class RestoreCommandTopicIntegrationTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static KsqlConfig KSQL_CONFIG;
  private static File BACKUP_LOCATION;
  private static TestKsqlRestApp REST_APP;
  private static String COMMAND_TOPIC;
  private static Path BACKUP_FILE;
  private static Path PROPERTIES_FILE;

  @BeforeClass
  public static void setup() throws IOException {
    BACKUP_LOCATION = TMP_FOLDER.newFolder();

    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        .withProperty(KSQL_METASTORE_BACKUP_LOCATION, BACKUP_LOCATION.getPath())
        .build();

    REST_APP.start();

    KSQL_CONFIG = new KsqlConfig(REST_APP.getKsqlRestConfig().getKsqlConfigProperties());
    COMMAND_TOPIC = ReservedInternalTopics.commandTopic(KSQL_CONFIG);
    BACKUP_FILE = Files.list(BACKUP_LOCATION.toPath()).findFirst().get();
    PROPERTIES_FILE = TMP_FOLDER.newFile().toPath();

    writeServerProperties();
  }

  @AfterClass
  public static void teardown() {
    REST_APP.stop();
    TMP_FOLDER.delete();
  }

  private static void writeServerProperties() throws IOException {
    final Map<String, Object> map = REST_APP.getKsqlRestConfig().getKsqlConfigProperties();

    Files.write(
        PROPERTIES_FILE,
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
    TEST_HARNESS.deleteTopics(Collections.singletonList(COMMAND_TOPIC));
    assertThat("Server should be in degraded state", isDegradedState(), is(true));

    // Restore the command topic
    KsqlRestoreCommandTopic.main(
        new String[]{
            "--yes",
            "--config-file", PROPERTIES_FILE.toString(),
            BACKUP_FILE.toString()
        });

    // Re-load the command topic
    REST_APP.stop();
    REST_APP.start();

    // Then
    final List<String> streamsNames = showStreams();
    assertThat("Should have TOPIC1", streamsNames.contains("TOPIC1"), is(true));
    assertThat("Should have TOPIC2", streamsNames.contains("TOPIC2"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Server should NOT be in degraded state", isDegradedState(), is(false));
  }

  private boolean isDegradedState() {
    // If in degraded state, then the following command will return a warning
    final List<KsqlEntity> response = makeKsqlRequest(
        "CREATE STREAM ANY (id INT) WITH (KAFKA_TOPIC='topic1', VALUE_FORMAT='JSON');");

    final List<KsqlWarning> warnings = response.get(0).getWarnings();
    return warnings.size() > 0 && warnings.get(0).getMessage()
        .contains("The server has detected corruption in the command topic");
  }

  private List<String> showStreams() {
    return ((StreamsList)makeKsqlRequest("SHOW STREAMS;").get(0))
        .getStreams().stream().map(SourceInfo::getName).collect(Collectors.toList());
  }

  private List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }
}
