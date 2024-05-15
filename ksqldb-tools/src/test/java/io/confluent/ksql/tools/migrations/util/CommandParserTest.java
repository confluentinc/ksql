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

package io.confluent.ksql.tools.migrations.util;

import static io.confluent.ksql.tools.migrations.util.CommandParser.toFieldType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.DefineVariable;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

public class CommandParserTest {

  @Test
  public void shouldParseInsertValuesStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO FOO VALUES (55);");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("FOO"));
    assertThat(insertValues.getColumns(), is(Collections.emptyList()));
    assertThat(insertValues.getValues().size(), is(1));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
  }

  @Test
  public void shouldParseInsertNullValuesStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO FOO VALUES (NULL);");

    // Then:
    assertThat(commands.size(), is(1));
      assertThat(commands.get(0).getStatement().isPresent(), is (true));
      assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("FOO"));
    assertThat(insertValues.getColumns(), is(Collections.emptyList()));
    assertThat(insertValues.getValues().size(), is(1));
    assertNull(toFieldType(insertValues.getValues().get(0)));
  }

  @Test
  public void shouldParseInsertValuesStatementWithExplicitFields() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO `foo` (col1, col2) VALUES (55, '40');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("foo"));
    assertThat(insertValues.getColumns(), is(ImmutableList.of(ColumnName.of("COL1"), ColumnName.of("COL2"))));
    assertThat(insertValues.getValues().size(), is(2));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
    assertThat(toFieldType(insertValues.getValues().get(1)), is("40"));
  }
//
  @Test
  public void shouldParseInsertValuesStatementWithoutSpaces() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO `foo`(col1, col2) VALUES(55, '40');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("foo"));
    assertThat(insertValues.getColumns(), is(ImmutableList.of(ColumnName.of("COL1"), ColumnName.of("COL2"))));
    assertThat(insertValues.getValues().size(), is(2));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
    assertThat(toFieldType(insertValues.getValues().get(1)), is("40"));
  }

  @Test
  public void shouldParseInsertValuesStatementWithExplicitQuoting() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO `foo` (`col1`) VALUES (55);");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("foo"));
    assertThat(insertValues.getColumns(), is(ImmutableList.of(ColumnName.of("col1"))));
    assertThat(insertValues.getValues().size(), is(1));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
  }

  @Test
  public void shouldParseInsertValuesStatementWithVariables() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse(
        "INSERT INTO ${stream} VALUES (${num});",
        ImmutableMap.of(
            "stream", "FOO",
            "num", "55"
        )
    );

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    final InsertValues insertValues = (InsertValues) commands.get(0).getStatement().get();

    assertThat(insertValues.getTarget().text(), is("FOO"));
    assertThat(insertValues.getColumns().size(), is(0));
    assertThat(insertValues.getValues().size(), is(1));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
  }

  @Test
  public void shouldThrowOnInvalidInsertValues() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("insert into foo values (this_should_not_here) ('val');"));

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse the statement"));
  }

  @Test
  public void shouldParseInsertIntoStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO FOO SELECT * FROM BAR;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("INSERT INTO FOO SELECT * FROM BAR;"));
  }

  @Test
  public void shouldParseSetUnsetStatements() {
    List<CommandParser.ParsedCommand> commands = parse("SeT 'foo.property'='bar';UnSET 'foo.property';");
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(SetProperty.class));
    assertThat(commands.get(0).getCommand(), is("SeT 'foo.property'='bar';"));
    assertThat(((SetProperty)commands.get(0).getStatement().get()).getPropertyName(), is("foo.property"));
    assertThat(((SetProperty)commands.get(0).getStatement().get()).getPropertyValue(), is("bar"));
    assertThat(commands.get(1).getStatement().isPresent(), is (true));
    assertThat(commands.get(1).getStatement().get(), instanceOf(UnsetProperty.class));
    assertThat(commands.get(1).getCommand(), is("UnSET 'foo.property';"));
    assertThat(((UnsetProperty)commands.get(1).getStatement().get()).getPropertyName(), is("foo.property"));
  }

  @Test
  public void shouldParseSetStatementsWithVariables() {
    List<CommandParser.ParsedCommand> commands = parse("SET '${name}'='${value}';",
        ImmutableMap.of("name", "foo.property", "value", "bar"));
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(SetProperty.class));
    assertThat(commands.get(0).getCommand(), is("SET 'foo.property'='bar';"));
    assertThat(((SetProperty)commands.get(0).getStatement().get()).getPropertyName(), is("foo.property"));
    assertThat(((SetProperty)commands.get(0).getStatement().get()).getPropertyValue(), is("bar"));
  }

  @Test
  public void shouldParseUnsetStatementsWithVariables() {
    List<CommandParser.ParsedCommand> commands = parse("UnSeT '${name}';",
        ImmutableMap.of("name", "foo.property"));
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(UnsetProperty.class));
    assertThat(((UnsetProperty)commands.get(0).getStatement().get()).getPropertyName(), is("foo.property"));
  }

  @Test
  public void shouldParseMultipleStatements() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("INSERT INTO foo VALUES (32);INSERT INTO FOO_2 VALUES ('wow',3,'hello ''world''!');");

    // Then:
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(0).getStatement().get()).getTarget().text(), is("FOO"));
    assertThat(((InsertValues) commands.get(0).getStatement().get()).getValues().size(), is(1));
    assertThat(toFieldType(((InsertValues) commands.get(0).getStatement().get()).getValues().get(0)), is(32));

    assertThat(commands.get(1).getStatement().isPresent(), is (true));
    assertThat(commands.get(1).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getTarget().text(), is("FOO_2"));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(0)), is("wow"));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(1)), is(3));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(2)), is("hello 'world'!"));
  }

  @Test
  public void shouldParseCreateStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');"));
  }

  @Test
  public void shouldParseCreateAsStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("CREATE STREAM FOO AS SELECT col1, col2 + 2 FROM BAR;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM FOO AS SELECT col1, col2 + 2 FROM BAR;"));
  }

  @Test
  public void shouldParseCreateOrReplaceStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("create or replace stream FOO (A STRING) WITH (KAFKA_TOPIC='FOO', VALUE_FORMAT='DELIMITED');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("create or replace stream FOO (A STRING) WITH (KAFKA_TOPIC='FOO', VALUE_FORMAT='DELIMITED');"));
  }

  @Test
  public void shouldParseTerminateStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("terminate some_query_id;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("terminate some_query_id;"));
  }

  @Test
  public void shouldParsePauseStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("pause some_query_id;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is("pause some_query_id;"));
  }

  @Test
  public void shouldParseResumeStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("resume some_query_id;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is("resume some_query_id;"));
  }

  @Test
  public void shouldParseDropSourceStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("drop stream foo;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("drop stream foo;"));
  }

  @Test
  public void shouldParseAlterSourceStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("alter stream foo add column new_col string;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("alter stream foo add column new_col string;"));
  }

  @Test
  public void shouldParseCreateTypeStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("create type address as struct<street varchar, number int, city string, zip varchar>;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("create type address as struct<street varchar, number int, city string, zip varchar>;"));
  }

  @Test
  public void shouldParseDropTypeStatement() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("drop type address;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("drop type address;"));
  }

  @Test
  public void shouldParseSeveralCommands() {
    // When:
    List<CommandParser.ParsedCommand> commands = parse("CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);\n"
        + "INSERT INTO `riderLocations` (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);\n"
        + "INSERT INTO \"riderLocations\" (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);\n"
        + "INSERT INTO `values` (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);");

    // Then:
    assertThat(commands.size(), is(7));
    assertThat(commands.get(0).getStatement().isPresent(), is (false));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);"));
    assertThat(commands.get(1).getStatement().isPresent(), is (true));
    assertThat(commands.get(1).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getTarget().text(), is("RIDERLOCATIONS"));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getColumns().size(), is(3));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getColumns().get(0), is(ColumnName.of("PROFILEID")));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getColumns().get(1), is(ColumnName.of("LATITUDE")));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getColumns().get(2), is(ColumnName.of("LONGITUDE")));
    assertThat(((InsertValues) commands.get(1).getStatement().get()).getValues().size(), is(3));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(0)), is("c2309eec"));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(1)), is(BigDecimal.valueOf(37.7877)));
    assertThat(toFieldType(((InsertValues) commands.get(1).getStatement().get()).getValues().get(2)), is(BigDecimal.valueOf(-122.4205)));
    assertThat(commands.get(2).getStatement().isPresent(), is (true));
    assertThat(commands.get(2).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(2).getStatement().get()).getTarget().text(), is("riderLocations"));
    assertThat(commands.get(3).getStatement().isPresent(), is (true));
    assertThat(commands.get(3).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(3).getStatement().get()).getTarget().text(), is("riderLocations"));
    assertThat(commands.get(4).getStatement().isPresent(), is (true));
    assertThat(commands.get(4).getStatement().get(), instanceOf(InsertValues.class));
    assertThat(((InsertValues) commands.get(4).getStatement().get()).getTarget().text(), is("values"));
  }

  @Test
  public void shouldParseCreateConnectorStatement() {
    // Given:
    final String createConnector = "CREATE SOURCE CONNECTOR `jdbc-connector` WITH(\n"
        + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
        + "    \"connection.url\"='jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(createConnector);

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(CreateConnector.class));
    assertThat(commands.get(0).getCommand(), is(createConnector));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getName(), is("jdbc-connector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getType() == CreateConnector.Type.SOURCE, is(true));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().size(), is(6));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connector.class").getValue(),
            is("io.confluent.connect.jdbc.JdbcSourceConnector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connection.url").getValue(),
            is("jdbc:postgresql://localhost:5432/my.db"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("mode").getValue(), is("bulk"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("topic.prefix").getValue(), is("jdbc-"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("table.whitelist").getValue(), is("users"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("key").getValue(), is("username"));
  }

  @Test
  public void shouldParseCreateConnectorStatementWithVariables() {
    // Given:
    final String createConnector = "CREATE SOURCE CONNECTOR ${name} WITH(\n"
        + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
        + "    \"connection.url\"=${url},\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";


            // When:
    List<CommandParser.ParsedCommand> commands = parse(createConnector, ImmutableMap.of("url", "'jdbc:postgresql://localhost:5432/my.db'", "name", "`jdbc_connector`"));

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(CreateConnector.class));
    assertThat(commands.get(0).getCommand(), is("CREATE SOURCE CONNECTOR `jdbc_connector` WITH(\n"
            + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
            + "    \"connection.url\"='jdbc:postgresql://localhost:5432/my.db',\n"
            + "    \"mode\"='bulk',\n"
            + "    \"topic.prefix\"='jdbc-',\n"
            + "    \"table.whitelist\"='users',\n"
            + "    \"key\"='username');"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getName(), is("jdbc_connector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getType() == CreateConnector.Type.SOURCE, is(true));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().size(), is(6));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connector.class").getValue(), is("io.confluent.connect.jdbc.JdbcSourceConnector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connection.url").getValue(), is("jdbc:postgresql://localhost:5432/my.db"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("mode").getValue(), is("bulk"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("topic.prefix").getValue(), is("jdbc-"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("table.whitelist").getValue(), is("users"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("key").getValue(), is("username"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).ifNotExists(), is(false));
  }

  @Test
  public void shouldParseCreateConnectorIfNotExistsStatement() {
    // Given:
    final String createConnector = "CREATE SOURCE CONNECTOR IF NOT EXISTS ${name} WITH(\n"
        + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
        + "    \"connection.url\"=${url},\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(createConnector, ImmutableMap.of("url", "'jdbc:postgresql://localhost:5432/my.db'", "name", "`jdbc_connector`"));

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getCommand(), is("CREATE SOURCE CONNECTOR IF NOT EXISTS `jdbc_connector` WITH(\n"
            + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
            + "    \"connection.url\"='jdbc:postgresql://localhost:5432/my.db',\n"
            + "    \"mode\"='bulk',\n"
            + "    \"topic.prefix\"='jdbc-',\n"
            + "    \"table.whitelist\"='users',\n"
            + "    \"key\"='username');"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getName(), is("jdbc_connector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getType() == CreateConnector.Type.SOURCE, is(true));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().size(), is(6));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connector.class").getValue(), is("io.confluent.connect.jdbc.JdbcSourceConnector"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("connection.url").getValue(), is("jdbc:postgresql://localhost:5432/my.db"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("mode").getValue(), is("bulk"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("topic.prefix").getValue(), is("jdbc-"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("table.whitelist").getValue(), is("users"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).getConfig().get("key").getValue(), is("username"));
    assertThat(((CreateConnector) commands.get(0).getStatement().get()).ifNotExists(), is(true));
  }

  @Test
  public void shouldParseDropConnectorStatement() {
    // Given:
    final String dropConnector = "DRoP CONNEcTOR `jdbc-connector` ;"; // The space at the end is intentional

    // When:
    List<CommandParser.ParsedCommand> commands = parse(dropConnector);

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is(dropConnector));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(DropConnector.class));
    assertThat(((DropConnector) commands.get(0).getStatement().get()).getConnectorName(), is("jdbc-connector"));
    assertThat(((DropConnector) commands.get(0).getStatement().get()).getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropConnectorIfExistsStatement() {
    // Given:
    final String dropConnector = "DRoP CONNEcTOR IF EXISTS `jdbc-connector` ;";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(dropConnector);

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is(dropConnector));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(DropConnector.class));
    assertThat(((DropConnector) commands.get(0).getStatement().get()).getConnectorName(), is("jdbc-connector"));
    assertThat(((DropConnector) commands.get(0).getStatement().get()).getIfExists(), is(true));
  }

  @Test
  public void shouldSplitCommandsWithComments() {
    // When:
    List<String> commands = CommandParser.splitSql("-- Before\n"
        + "CREATE STREAM valid_purchases AS\n"
        + "  SELECT *\n"
        + "  FROM purchases\n"
        + "  WHERE cost > 0.00 AND quantity > 0;\n"
        + "-- After\n"
        + "CREATE OR REPLACE STREAM valid_purchases AS\n"
        + "  SELECT *\n"
        + "  FROM purchases\n"
        + "  WHERE quantity > 0;"
        + "/* Let's insert some values now! -- it's fun! */\n"
        + "INSERT INTO purchases VALUES ('c''ow', -90);\n"
        + "INSERT INTO purchases VALUES ('/*she*/ep',     80);\n"
        + "INSERT INTO purchases VALUES ('pol/*ar;;be--ar*/;', 200000);");

    // Then:
    assertThat(commands.size(), is(5));
    assertThat(commands.get(0), is("CREATE STREAM valid_purchases AS\n  SELECT *\n  FROM purchases\n  WHERE cost > 0.00 AND quantity > 0;"));
    assertThat(commands.get(1), is("\nCREATE OR REPLACE STREAM valid_purchases AS\n  SELECT *\n  FROM purchases\n  WHERE quantity > 0;"));
    assertThat(commands.get(2), is("\nINSERT INTO purchases VALUES ('c''ow', -90);"));
    assertThat(commands.get(3), is("\nINSERT INTO purchases VALUES ('/*she*/ep',     80);"));
    assertThat(commands.get(4), is("\nINSERT INTO purchases VALUES ('pol/*ar;;be--ar*/;', 200000);"));
  }

  @Test
  public void shouldParseDefineStatement() {
    // Given:
    final String defineVar = "DEFINE var = 'foo';";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(defineVar);

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is(defineVar));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(DefineVariable.class));
    assertThat(((DefineVariable) commands.get(0).getStatement().get()).getVariableName(), is("var"));
    assertThat(((DefineVariable) commands.get(0).getStatement().get()).getVariableValue(), is("foo"));
  }

  @Test
  public void shouldDefineStatementWithVariable() {
    // Given:
    final String defineVar = "DEFiNe word = 'walk${suffix}';";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(defineVar, ImmutableMap.of("suffix", "ing"));

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(DefineVariable.class));
    assertThat(((DefineVariable) commands.get(0).getStatement().get()).getVariableName(), is("word"));
    assertThat(((DefineVariable) commands.get(0).getStatement().get()).getVariableValue(), is("walking"));
  }

  @Test
  public void shouldParseUndefineStatement() {
    // Given:
    final String undefineVar = "UNDEFINE var;";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(undefineVar);

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).getCommand(), is(undefineVar));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(UndefineVariable.class));
    assertThat(((UndefineVariable) commands.get(0).getStatement().get()).getVariableName(), is("var"));
  }

  @Test
  public void shouldThrowOnMalformedComment() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.splitSql("/* Comment "));

    // Then:
    assertThat(e.getMessage(), is("Invalid sql - failed to find closing token '*/'"));
  }

  @Test
  public void shouldThrowOnMalformedQuote() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.splitSql("select 'unclosed quote;"));

    // Then:
    assertThat(e.getMessage(), is("Invalid sql - failed to find closing token '''"));
  }

  @Test
  public void shouldThrowOnMissingSemicolon() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("create stream foo as select * from no_semicolon_after_this"));

    // Then:
    assertThat(e.getMessage(), containsString("Unmatched command at end of file; missing semicolon"));
  }

  @Test
  public void shouldThrowOnDescribeStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("describe my_stream;"));

    // Then:
    assertThat(e.getMessage(), is("'DESCRIBE' statements are not supported."));
  }

  @Test
  public void shouldThrowOnExplainStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("explain my_query_id;"));

    // Then:
    assertThat(e.getMessage(), is("'EXPLAIN' statements are not supported."));
  }

  @Test
  public void shouldThrowOnSelectStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("select * from my_stream emit changes;"));

    // Then:
    assertThat(e.getMessage(), is("'SELECT' statements are not supported."));
  }

  @Test
  public void shouldThrowOnPrintStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("print 'my_topic';"));

    // Then:
    assertThat(e.getMessage(), is("'PRINT' statements are not supported."));
  }

  @Test
  public void shouldThrowOnShowStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("show connectors;"));

    // Then:
    assertThat(e.getMessage(), is("'SHOW' statements are not supported."));
  }

  @Test
  public void shouldThrowOnListStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("list queries;"));

    // Then:
    assertThat(e.getMessage(), is("'LIST' statements are not supported."));
  }

  @Test
  public void shouldThrowOnRunScriptStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> parse("RUN SCRIPT 'my_script.sql';"));

    // Then:
    assertThat(e.getMessage(), is("'RUN SCRIPT' statements are not supported."));
  }

  @Test
  public void shouldParseAssertTopic() {
    // Given:
    final String assertTopics = "assert topic abc; assert not exists topic 'abcd' with (foo=2, bar=3) timeout 4 minutes;"
        + "assert topic ${topic} with (replicas=${replicas}, partitions=${partitions}) timeout 10 seconds;";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(assertTopics, ImmutableMap.of("replicas", "3", "partitions", "5", "topic", "name"));

    // Then:
    assertThat(commands.size(), is(3));
    assertThat(commands.get(0).getCommand(), is("assert topic abc;"));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(AssertTopic.class));
    assertThat(((AssertTopic) commands.get(0).getStatement().get()).getTopic(), is("abc"));
    assertThat(((AssertTopic) commands.get(0).getStatement().get()).getConfig().size(), is(0));
    assertThat(((AssertTopic) commands.get(0).getStatement().get()).checkExists(), is(true));
    assertThat(((AssertTopic) commands.get(0).getStatement().get()).getTimeout(), is(Optional.empty()));

    assertThat(commands.get(1).getCommand(), is( "assert not exists topic 'abcd' with (foo=2, bar=3) timeout 4 minutes;"));
    assertThat(commands.get(1).getStatement().isPresent(), is (true));
    assertThat(commands.get(1).getStatement().get(), instanceOf(AssertTopic.class));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).getTopic(), is("abcd"));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).getConfig().size(), is(2));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).getConfig().get("FOO").getValue(), is(2));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).getConfig().get("BAR").getValue(), is(3));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).checkExists(), is(false));
    assertThat(((AssertTopic) commands.get(1).getStatement().get()).getTimeout().get(), is(WindowTimeClause.of(4, TimeUnit.MINUTES.name())));

    assertThat(commands.get(2).getCommand(), is( "assert topic name with (replicas=3, partitions=5) timeout 10 seconds;"));
    assertThat(commands.get(2).getStatement().isPresent(), is (true));
    assertThat(commands.get(2).getStatement().get(), instanceOf(AssertTopic.class));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).getTopic(), is("name"));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).getConfig().size(), is(2));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).getConfig().get("REPLICAS").getValue(), is(3));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).getConfig().get("PARTITIONS").getValue(), is(5));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).checkExists(), is(true));
    assertThat(((AssertTopic) commands.get(2).getStatement().get()).getTimeout().get(), is(WindowTimeClause.of(10, TimeUnit.SECONDS.name())));
  }

  @Test
  public void shouldParseAssertSchema() {
    // Given:
    final String assertTopics = "assert schema id 3; assert not exists schema subject 'abcd' timeout 4 minutes;"
        + "assert schema subject ${subject} id ${id} timeout 10 seconds;";

    // When:
    List<CommandParser.ParsedCommand> commands = parse(assertTopics, ImmutableMap.of("subject", "name", "id", "4"));

    // Then:
    assertThat(commands.size(), is(3));
    assertThat(commands.get(0).getCommand(), is("assert schema id 3;"));
    assertThat(commands.get(0).getStatement().isPresent(), is (true));
    assertThat(commands.get(0).getStatement().get(), instanceOf(AssertSchema.class));
    assertThat(((AssertSchema) commands.get(0).getStatement().get()).getSubject(), is(Optional.empty()));
    assertThat(((AssertSchema) commands.get(0).getStatement().get()).getId().get(), is(3));
    assertThat(((AssertSchema) commands.get(0).getStatement().get()).checkExists(), is(true));
    assertThat(((AssertSchema) commands.get(0).getStatement().get()).getTimeout(), is(Optional.empty()));

    assertThat(commands.get(1).getCommand(), is( "assert not exists schema subject 'abcd' timeout 4 minutes;"));
    assertThat(commands.get(1).getStatement().isPresent(), is (true));
    assertThat(commands.get(1).getStatement().get(), instanceOf(AssertSchema.class));
    assertThat(((AssertSchema) commands.get(1).getStatement().get()).getSubject().get(), is("abcd"));
    assertThat(((AssertSchema) commands.get(1).getStatement().get()).getId(), is(Optional.empty()));
    assertThat(((AssertSchema) commands.get(1).getStatement().get()).checkExists(), is(false));
    assertThat(((AssertSchema) commands.get(1).getStatement().get()).getTimeout().get(), is(WindowTimeClause.of(4, TimeUnit.MINUTES.name())));

    assertThat(commands.get(2).getCommand(), is( "assert schema subject name id 4 timeout 10 seconds;"));
    assertThat(commands.get(2).getStatement().isPresent(), is (true));
    assertThat(commands.get(2).getStatement().get(), instanceOf(AssertSchema.class));
    assertThat(((AssertSchema) commands.get(2).getStatement().get()).getSubject().get(), is("name"));
    assertThat(((AssertSchema) commands.get(2).getStatement().get()).getId().get(), is(4));
    assertThat(((AssertSchema) commands.get(2).getStatement().get()).checkExists(), is(true));
    assertThat(((AssertSchema) commands.get(2).getStatement().get()).getTimeout().get(), is(WindowTimeClause.of(10, TimeUnit.SECONDS.name())));
  }

  private List<CommandParser.ParsedCommand> parse(final String sql, Map<String, String> variables) {
    return CommandParser.splitSql(sql).stream()
        .map(command -> CommandParser.parse(command, variables))
        .collect(Collectors.toList());
  }

  private List<CommandParser.ParsedCommand> parse(final String sql) {
    return parse(sql, ImmutableMap.of());
  }
}
