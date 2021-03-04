/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import com.github.rvesse.airline.Cli;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.tools.migrations.commands.BaseCommand;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category({IntegrationTest.class})
public class MigrationsTest {

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private static final Cli<BaseCommand> MIGRATIONS_CLI = new Cli<>(Migrations.class);

  private static final String MIGRATIONS_STREAM = "custom_migration_stream_name";
  private static final String MIGRATIONS_TABLE = "custom_migration_table_name";

  @Mock
  private AppenderSkeleton logAppender;
  @Captor
  private ArgumentCaptor<LoggingEvent> logCaptor;

  private static String configFilePath;

  @BeforeClass
  public static void setUpClass() throws Exception {
    final String testDir = Paths.get(TestUtils.tempDirectory().getAbsolutePath(), "migrations_integ_test").toString();
    createAndVerifyDirectoryStructure(testDir);

    configFilePath = Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE).toString();

    writeAdditionalConfigs(ImmutableMap.of(
        MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME, MIGRATIONS_STREAM,
        MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME, MIGRATIONS_TABLE
    ));
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  @Before
  public void setUp() {
    initializeAndVerifyMetadataStreamAndTable(configFilePath);
    waitForMetadataTableReady();
  }

  @After
  public void tearDown() {
    cleanAndVerify(configFilePath);
  }

  @Test
  public void shouldApplyMigrationsAndDisplayInfo() throws Exception {
    shouldApplyMigrations();
    shouldDisplayInfo();
  }

  private void shouldApplyMigrations() throws Exception {
    // Given:
    createMigrationFile(
        1,
        "foo FOO fO0",
        configFilePath,
        "CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');"
    );
    createMigrationFile(
        2,
        "bar_bar_BAR",
        configFilePath,
        "CREATE STREAM BAR (A STRING) WITH (KAFKA_TOPIC='BAR', PARTITIONS=1, VALUE_FORMAT='DELIMITED');"
    );

    // When:
    final int applyStatus = MIGRATIONS_CLI.parse("--config-file", configFilePath, "apply", "-a").run();

    // Then:
    assertThat(applyStatus, is(0));

    verifyMigrationsApplied();
  }

  private void shouldDisplayInfo() {
    // Given:
    Logger.getRootLogger().addAppender(logAppender);

    try {
      // When:
      final int infoStatus = MIGRATIONS_CLI.parse("--config-file", configFilePath, "info").run();

      // Then:
      assertThat(infoStatus, is(0));

      verify(logAppender, atLeastOnce()).doAppend(logCaptor.capture());
      final List<String> logMessages = logCaptor.getAllValues().stream()
          .map(LoggingEvent::getRenderedMessage)
          .collect(Collectors.toList());
      assertThat(logMessages, hasItem(containsString("Current migration version: 2")));
      assertThat(logMessages, hasItem(matchesRegex(
          " Version \\| Name        \\| State    \\| Previous Version \\| Started On\\s+\\| Completed On\\s+\\| Error Reason \n" +
              "-+\n" +
              " 1       \\| foo FOO fO0 \\| MIGRATED \\| <none>           \\| \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\S+ \\| \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\S+ \\| N/A          \n" +
              " 2       \\| bar bar BAR \\| MIGRATED \\| 1                \\| \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\S+ \\| \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\S+ \\| N/A          \n" +
              "-+\n"
      )));
    } finally {
      Logger.getRootLogger().removeAppender(logAppender);
    }
  }

  private static void verifyMigrationsApplied() {
    // verify FOO and BAR were registered
    describeSource("FOO");
    describeSource("BAR");

    // verify version 1
    final List<StreamedRow> version1 = assertThatEventually(
        () -> makeKsqlQuery("SELECT * FROM " + MIGRATIONS_TABLE + " WHERE VERSION_KEY='1';"),
        hasSize(2));
    assertThat(version1.get(1).getRow().get().getColumns().get(1), is("1"));
    assertThat(version1.get(1).getRow().get().getColumns().get(2), is("foo FOO fO0"));
    assertThat(version1.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThat(version1.get(1).getRow().get().getColumns().get(7), is("<none>"));

    // verify version 2
    final List<StreamedRow> version2 = assertThatEventually(
        () -> makeKsqlQuery("SELECT * FROM " + MIGRATIONS_TABLE + " WHERE VERSION_KEY='CURRENT';"),
        hasSize(2));
    assertThat(version2.get(1).getRow().get().getColumns().get(1), is("2"));
    assertThat(version2.get(1).getRow().get().getColumns().get(2), is("bar bar BAR"));
    assertThat(version2.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThat(version2.get(1).getRow().get().getColumns().get(7), is("1"));

    // verify current
    final List<StreamedRow> current =
        makeKsqlQuery("SELECT * FROM " + MIGRATIONS_TABLE + " WHERE VERSION_KEY='CURRENT';");
    assertThat(current.size(), is(2));
    assertThat(current.get(1).getRow().get().getColumns().get(1), is("2"));
    assertThat(current.get(1).getRow().get().getColumns().get(2), is("bar bar BAR"));
    assertThat(current.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThat(current.get(1).getRow().get().getColumns().get(7), is("1"));
  }

  private static void createAndVerifyDirectoryStructure(final String testDir) throws Exception {
    // use `new` to create directory structure
    final int status = MIGRATIONS_CLI.parse("new", testDir, REST_APP.getHttpListener().toString()).run();
    assertThat(status, is(0));

    // verify root directory
    final File rootDir = new File(testDir);
    assertThat(rootDir.exists(), is(true));
    assertThat(rootDir.isDirectory(), is(true));

    // verify migrations directory
    final File migrationsDir = new File(Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_DIR).toString());
    assertThat(migrationsDir.exists(), is(true));
    assertThat(migrationsDir.isDirectory(), is(true));

    // verify config file
    final File configFile = new File(Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE).toString());
    assertThat(configFile.exists(), is(true));
    assertThat(configFile.isDirectory(), is(false));

    // verify config file contents
    final List<String> lines = Files.readAllLines(configFile.toPath());
    assertThat(lines, hasSize(1));
    assertThat(lines.get(0), is(MigrationConfig.KSQL_SERVER_URL + "=" + REST_APP.getHttpListener().toString()));
  }

  private static void writeAdditionalConfigs(final Map<String, String> additionalConfigs) throws Exception {
    try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(configFilePath, true), StandardCharsets.UTF_8))) {
      for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
        out.println(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  private static void initializeAndVerifyMetadataStreamAndTable(final String configFile) {
    // use `initialize` to create metadata stream and table
    final int status = MIGRATIONS_CLI.parse("--config-file", configFile, "initialize").run();
    assertThat(status, is(0));

    // verify metadata stream
    final SourceDescription streamDesc = describeSource(MIGRATIONS_STREAM);
    assertThat(streamDesc.getType(), is("STREAM"));
    assertThat(streamDesc.getTopic(), is("default_ksql_" + MIGRATIONS_STREAM));
    assertThat(streamDesc.getKeyFormat(), is("KAFKA"));
    assertThat(streamDesc.getValueFormat(), is("JSON"));
    assertThat(streamDesc.getPartitions(), is(1));
    assertThat(streamDesc.getReplication(), is(1));
    assertThat(streamDesc.getFields(), containsInAnyOrder(
        fieldInfo("VERSION_KEY", "STRING", true),
        fieldInfo("VERSION", "STRING", false),
        fieldInfo("NAME", "STRING", false),
        fieldInfo("STATE", "STRING", false),
        fieldInfo("CHECKSUM", "STRING", false),
        fieldInfo("STARTED_ON", "STRING", false),
        fieldInfo("COMPLETED_ON", "STRING", false),
        fieldInfo("PREVIOUS", "STRING", false),
        fieldInfo("ERROR_REASON", "STRING", false)
    ));

    // verify metadata table
    final SourceDescription tableDesc = describeSource(MIGRATIONS_TABLE);
    assertThat(tableDesc.getType(), is("TABLE"));
    assertThat(tableDesc.getTopic(), is("default_ksql_" + MIGRATIONS_TABLE));
    assertThat(tableDesc.getKeyFormat(), is("KAFKA"));
    assertThat(tableDesc.getValueFormat(), is("JSON"));
    assertThat(tableDesc.getPartitions(), is(1));
    assertThat(tableDesc.getReplication(), is(1));
    assertThat(tableDesc.getFields(), containsInAnyOrder(
        fieldInfo("VERSION_KEY", "STRING", true),
        fieldInfo("VERSION", "STRING", false),
        fieldInfo("NAME", "STRING", false),
        fieldInfo("STATE", "STRING", false),
        fieldInfo("CHECKSUM", "STRING", false),
        fieldInfo("STARTED_ON", "STRING", false),
        fieldInfo("COMPLETED_ON", "STRING", false),
        fieldInfo("PREVIOUS", "STRING", false),
        fieldInfo("ERROR_REASON", "STRING", false)
    ));
  }

  private static void cleanAndVerify(final String configFile) {
    // Given:
    assertThat(sourceExists(MIGRATIONS_STREAM, false), is(true));
    assertThat(sourceExists(MIGRATIONS_TABLE, true), is(true));

    // When: use `clean` to clean up metadata stream and table
    final int status = MIGRATIONS_CLI.parse("--config-file", configFile, "clean").run();
    assertThat(status, is(0));

    // Then:
    assertThatEventually(() -> sourceExists(MIGRATIONS_STREAM, false), is(false));
    assertThat(sourceExists(MIGRATIONS_TABLE, true), is(false));
  }

  private static boolean sourceExists(final String sourceName, final boolean isTable) {
    final String sourceType = isTable ? "TABLE" : "STREAM";
    final List<KsqlEntity> entities = makeKsqlRequest("LIST " + sourceType + "S;");
    assertThat(entities, hasSize(1));

    final Stream<String> names;
    if (isTable) {
      assertThat(entities.get(0), instanceOf(TablesList.class));
      names = ((TablesList) entities.get(0)).getTables().stream().map(SourceInfo.Table::getName);
    } else {
      assertThat(entities.get(0), instanceOf(StreamsList.class));
      names = ((StreamsList) entities.get(0)).getStreams().stream().map(SourceInfo.Stream::getName);
    }

    return names.anyMatch(n -> n.equalsIgnoreCase(sourceName));
  }

  private static SourceDescription describeSource(final String name) {
    final List<KsqlEntity> entities = assertThatEventually(
        () -> makeKsqlRequest("DESCRIBE " + name + ";"),
        hasSize(1));

    assertThat(entities.get(0), instanceOf(SourceDescriptionEntity.class));
    SourceDescriptionEntity entity = (SourceDescriptionEntity) entities.get(0);

    return entity.getSourceDescription();
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static List<StreamedRow> makeKsqlQuery(final String sql) {
    return RestIntegrationTestUtil.makeQueryRequest(REST_APP, sql, Optional.empty());
  }

  private static Matcher<? super String> matchesRegex(final String regex) {
    return new TypeSafeDiagnosingMatcher<String>() {
      @Override
      protected boolean matchesSafely(
          final String actual,
          final Description mismatchDescription) {
        return actual.matches(regex);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("matches regex: " + regex);
      }
    };
  }

  private static Matcher<? super FieldInfo> fieldInfo(
      final String name,
      final String type,
      final boolean isKey
  ) {
    return new TypeSafeDiagnosingMatcher<FieldInfo>() {
      @Override
      protected boolean matchesSafely(
          final FieldInfo actual,
          final Description mismatchDescription) {
        if (!name.equals(actual.getName())) {
          return false;
        }
        if (!type.equals(actual.getSchema().getTypeName())) {
          return false;
        }
        return isKey == isKey(actual);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(String.format(
            "name: %s. type: %s. isKey: %s",
            name, type, isKey));
      }

      private boolean isKey(final FieldInfo fieldInfo) {
        if (!fieldInfo.getType().isPresent()) {
          return false;
        }
        return fieldInfo.getType().get().equals(FieldInfo.FieldType.KEY);
      }
    };
  }

  private static void createMigrationFile(
      final int version,
      final String name,
      final String configFilePath,
      final String content
  ) throws IOException {
    // use `create` to create empty file
    final int status = MIGRATIONS_CLI.parse("--config-file", configFilePath,
        "create", name, "-v", String.valueOf(version)).run();
    assertThat(status, is(0));

    // validate file created
    final File filePath = new File(Paths.get(
        MigrationsDirectoryUtil.getMigrationsDirFromConfigFile(configFilePath),
        String.format("/V00000%d__%s.sql", version, name.replace(' ', '_'))
    ).toString());
    assertThat(filePath.exists(), is(true));
    assertThat(filePath.isDirectory(), is(false));

    // write contents to file
    try (PrintWriter out = new PrintWriter(filePath, Charset.defaultCharset().name())) {
      out.println(content);
    }
  }

  private static void waitForMetadataTableReady() {
    // This is needed to make sure that the table is fully done being created.
    // It's a similar situation to https://github.com/confluentinc/ksql/issues/6249
    assertThatEventually(
        () -> makeKsqlQuery("SELECT * FROM " + MIGRATIONS_TABLE + " WHERE VERSION_KEY='CURRENT';").size(),
        is(1)
    );
  }
}