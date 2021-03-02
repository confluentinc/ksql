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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.github.rvesse.airline.Cli;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.tools.migrations.commands.BaseCommand;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

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

  private static String configFilePath;

  @BeforeClass
  public static void setUpClass() throws Exception {
    final String testDir = Paths.get(TestUtils.tempDirectory().getAbsolutePath(), "migrations_integ_test").toString();
    createAndVerifyDirectoryStructure(testDir);

    configFilePath = Paths.get(testDir, MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE).toString();
    initializeAndVerifyMetadataStreamAndTable(configFilePath);
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  @Test
  public void testApply() throws IOException {
    // MigrationFile file
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

    // This is needed to make sure that the table is fully done being created.
    // It's a similar situation to https://github.com/confluentinc/ksql/issues/6249
    assertThatEventually(
        () -> makeKsqlQuery("SELECT * FROM migration_schema_versions WHERE VERSION_KEY='CURRENT';").size(),
        is(1)
    );
    final int status = MIGRATIONS_CLI.parse("--config-file", configFilePath, "apply", "-a").run();
    assertThat(status, is(0));

    // verify FOO and BAR were registered
    describeSource("FOO");
    describeSource("BAR");

    // verify current
    final List<StreamedRow> current = makeKsqlQuery("SELECT * FROM migration_schema_versions WHERE VERSION_KEY='CURRENT';");
    assertThatEventually(() -> current.size(), is(2));
    assertThatEventually(() -> current.get(1).getRow().get().getColumns().get(1), is("2"));
    assertThatEventually(() -> current.get(1).getRow().get().getColumns().get(2), is("bar bar BAR"));
    assertThatEventually(() -> current.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThatEventually(() -> current.get(1).getRow().get().getColumns().get(7), is("1"));

    // verify version 1
    final List<StreamedRow> version1 = makeKsqlQuery("SELECT * FROM migration_schema_versions WHERE VERSION_KEY='1';");
    assertThatEventually(() -> version1.size(), is(2));
    assertThatEventually(() -> version1.get(1).getRow().get().getColumns().get(1), is("1"));
    assertThatEventually(() -> version1.get(1).getRow().get().getColumns().get(2), is("foo FOO fO0"));
    assertThatEventually(() -> version1.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThatEventually(() -> version1.get(1).getRow().get().getColumns().get(7), is("<none>"));

    // verify version 2
    final List<StreamedRow> version2 = makeKsqlQuery("SELECT * FROM migration_schema_versions WHERE VERSION_KEY='CURRENT';");
    assertThatEventually(() -> version2.size(), is(2));
    assertThatEventually(() -> version2.get(1).getRow().get().getColumns().get(1), is("2"));
    assertThatEventually(() -> version2.get(1).getRow().get().getColumns().get(2), is("bar bar BAR"));
    assertThatEventually(() -> version2.get(1).getRow().get().getColumns().get(3), is("MIGRATED"));
    assertThatEventually(() -> version2.get(1).getRow().get().getColumns().get(7), is("1"));
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

  private static void initializeAndVerifyMetadataStreamAndTable(final String configFile) {
    // use `initialize` to create metadata stream and table
    final int status = MIGRATIONS_CLI.parse("--config-file", configFile, "initialize").run();
    assertThat(status, is(0));

    // verify metadata stream
    final SourceDescription streamDesc = describeSource("migration_events");
    assertThatEventually(() -> streamDesc.getType(), is("STREAM"));
    assertThatEventually(() -> streamDesc.getTopic(), is("default_ksql_migration_events"));
    assertThatEventually(() -> streamDesc.getKeyFormat(), is("KAFKA"));
    assertThatEventually(() -> streamDesc.getValueFormat(), is("JSON"));
    assertThatEventually(() -> streamDesc.getPartitions(), is(1));
    assertThatEventually(() -> streamDesc.getReplication(), is(1));
    assertThatEventually(() -> streamDesc.getFields(), containsInAnyOrder(
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
    final SourceDescription tableDesc = describeSource("migration_schema_versions");
    assertThatEventually(() -> tableDesc.getType(), is("TABLE"));
    assertThatEventually(() -> tableDesc.getTopic(), is("default_ksql_migration_schema_versions"));
    assertThatEventually(() -> tableDesc.getKeyFormat(), is("KAFKA"));
    assertThatEventually(() -> tableDesc.getValueFormat(), is("JSON"));
    assertThatEventually(() -> tableDesc.getPartitions(), is(1));
    assertThatEventually(() -> tableDesc.getReplication(), is(1));
    assertThatEventually(() -> tableDesc.getFields(), containsInAnyOrder(
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

  private static SourceDescription describeSource(final String name) {
    final List<KsqlEntity> entities = makeKsqlRequest("DESCRIBE " + name + ";");

    assertThatEventually(() -> entities, hasSize(1));
    assertThatEventually(() -> entities.get(0), instanceOf(SourceDescriptionEntity.class));
    SourceDescriptionEntity entity = (SourceDescriptionEntity) entities.get(0);

    return entity.getSourceDescription();
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static List<StreamedRow> makeKsqlQuery(final String sql) {
    return RestIntegrationTestUtil.makeQueryRequest(REST_APP, sql, Optional.empty());
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
}