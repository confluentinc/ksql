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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlConnectorStatement;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlInsertValues;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlPropertyCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlStatement;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CommandParserTest {

  @Test
  public void shouldParseInsertValuesStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO FOO VALUES (55);");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlInsertValues.class));
    final SqlInsertValues insertValues = (SqlInsertValues) commands.get(0);

    assertThat(insertValues.getSourceName(), is("FOO"));
    assertThat(insertValues.getColumns(), is(Collections.emptyList()));
    assertThat(insertValues.getValues().size(), is(1));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
  }

  @Test
  public void shouldParseInsertValuesStatementWithExplicitFields() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO `foo` (col1, col2) VALUES (55, '40');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlInsertValues.class));
    final SqlInsertValues insertValues = (SqlInsertValues) commands.get(0);

    assertThat(insertValues.getSourceName(), is("foo"));
    assertThat(insertValues.getColumns(), is(ImmutableList.of("COL1", "COL2")));
    assertThat(insertValues.getValues().size(), is(2));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
    assertThat(toFieldType(insertValues.getValues().get(1)), is("40"));
  }

  @Test
  public void shouldParseInsertValuesStatementWithExplicitQuoting() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO `foo` (`col1`) VALUES (55);");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlInsertValues.class));
    final SqlInsertValues insertValues = (SqlInsertValues) commands.get(0);

    assertThat(insertValues.getSourceName(), is("foo"));
    assertThat(insertValues.getColumns(), is(ImmutableList.of("col1")));
    assertThat(insertValues.getValues().size(), is(1));
    assertThat(toFieldType(insertValues.getValues().get(0)), is(55));
  }

  @Test
  public void shouldThrowOnInvalidInsertValues() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("insert into foo values (this_should_not_here) ('val');"));

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse INSERT VALUES statement"));
  }

  @Test
  public void shouldParseInsertIntoStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO FOO SELECT VALUES FROM BAR;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("INSERT INTO FOO SELECT VALUES FROM BAR;"));
  }

  @Test
  public void shouldParseSetUnsetStatements() {
    List<SqlCommand> commands = CommandParser.parse("SeT 'foo.property'='bar';UnSeT 'foo.property';");
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), instanceOf(SqlPropertyCommand.class));
    assertThat(commands.get(0).getCommand(), is("SeT 'foo.property'='bar';"));
    assertThat(((SqlPropertyCommand) commands.get(0)).isSetCommand(), is(true));
    assertThat(((SqlPropertyCommand) commands.get(0)).getProperty(), is("foo.property"));
    assertThat(((SqlPropertyCommand) commands.get(0)).getValue().get(), is("bar"));
    assertThat(commands.get(1), instanceOf(SqlPropertyCommand.class));
    assertThat(commands.get(1).getCommand(), is("UnSeT 'foo.property';"));
    assertThat(((SqlPropertyCommand) commands.get(1)).isSetCommand(), is(false));
    assertThat(((SqlPropertyCommand) commands.get(1)).getProperty(), is("foo.property"));
    assertTrue(!((SqlPropertyCommand) commands.get(1)).getValue().isPresent());
  }

  @Test
  public void shouldParseMultipleStatements() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO foo VALUES (32);INSERT INTO FOO_2 VALUES ('wow',3,'hello ''world''!');");

    // Then:
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(0)).getSourceName(), is("FOO"));
    assertThat(((SqlInsertValues) commands.get(0)).getValues().size(), is(1));
    assertThat(toFieldType(((SqlInsertValues) commands.get(0)).getValues().get(0)), is(32));

    assertThat(commands.get(1), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(1)).getSourceName(), is("FOO_2"));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(0)), is("wow"));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(1)), is(3));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(2)), is("hello 'world'!"));
  }

  @Test
  public void shouldParseCreateStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');"));
  }

  @Test
  public void shouldParseCreateAsStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("CREATE STREAM FOO AS SELECT col1, col2 + 2 FROM BAR;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM FOO AS SELECT col1, col2 + 2 FROM BAR;"));
  }

  @Test
  public void shouldParseCreateOrReplaceStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("create or replace stream FOO (A STRING) WITH (KAFKA_TOPIC='FOO', VALUE_FORMAT='DELIMITED');");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("create or replace stream FOO (A STRING) WITH (KAFKA_TOPIC='FOO', VALUE_FORMAT='DELIMITED');"));
  }

  @Test
  public void shouldParseTerminateStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("terminate some_query_id;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("terminate some_query_id;"));
  }

  @Test
  public void shouldParseDropSourceStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("drop stream foo;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("drop stream foo;"));
  }

  @Test
  public void shouldParseAlterSourceStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("alter stream foo add column new_col string;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("alter stream foo add column new_col string;"));
  }

  @Test
  public void shouldParseCreateTypeStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("create type address as struct<street varchar, number int, city string, zip varchar>;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("create type address as struct<street varchar, number int, city string, zip varchar>;"));
  }

  @Test
  public void shouldParseDropTypeStatement() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("drop type address;");

    // Then:
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("drop type address;"));
  }

  @Test
  public void shouldParseSeveralCommands() {
    // When:
    List<SqlCommand> commands = CommandParser.parse("CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);\n"
        + "INSERT INTO `riderLocations` (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);\n"
        + "INSERT INTO \"riderLocations\" (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);\n"
        + "INSERT INTO `values` (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);");

    // Then:
    assertThat(commands.size(), is(7));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);"));
    assertThat(commands.get(1), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(1)).getSourceName(), is("RIDERLOCATIONS"));
    assertThat(((SqlInsertValues) commands.get(1)).getColumns().size(), is(3));
    assertThat(((SqlInsertValues) commands.get(1)).getColumns().get(0), is("PROFILEID"));
    assertThat(((SqlInsertValues) commands.get(1)).getColumns().get(1), is("LATITUDE"));
    assertThat(((SqlInsertValues) commands.get(1)).getColumns().get(2), is("LONGITUDE"));
    assertThat(((SqlInsertValues) commands.get(1)).getValues().size(), is(3));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(0)), is("c2309eec"));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(1)), is(BigDecimal.valueOf(37.7877)));
    assertThat(toFieldType(((SqlInsertValues) commands.get(1)).getValues().get(2)), is(BigDecimal.valueOf(-122.4205)));
    assertThat(commands.get(2), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(2)).getSourceName(), is("riderLocations"));
    assertThat(commands.get(3), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(3)).getSourceName(), is("riderLocations"));
    assertThat(commands.get(4), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(4)).getSourceName(), is("values"));
  }

  @Test
  public void shouldParseConnectorStatements() {
    // Given:
    final String createConnector = "CREATE SOURCE CONNECTOR `jdbc-connector` WITH(\n"
        + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
        + "    \"connection.url\"='jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";
    final String dropConnector = "DROP CONNECTOR `jdbc-connector`;";

    // When:
    List<SqlCommand> commands = CommandParser.parse(createConnector + dropConnector);

    // Then:
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), instanceOf(SqlConnectorStatement.class));
    assertThat(commands.get(0).getCommand(), is(createConnector));
    assertThat(commands.get(1), instanceOf(SqlConnectorStatement.class));
    assertThat(commands.get(1).getCommand(), is(dropConnector));
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
        () -> CommandParser.parse("create stream foo as select * from no_semicolon_after_this"));

    // Then:
    assertThat(e.getMessage(), containsString("Unmatched command at end of file; missing semicolon"));
  }

  @Test
  public void shouldThrowOnDefineStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("define var = 'value';"));

    // Then:
    assertThat(e.getMessage(), is("'DEFINE' statements are not supported."));
  }

  @Test
  public void shouldThrowOnUndefineStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("undefine var;"));

    // Then:
    assertThat(e.getMessage(), is("'UNDEFINE' statements are not supported."));
  }

  @Test
  public void shouldThrowOnDescribeStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("describe my_stream;"));

    // Then:
    assertThat(e.getMessage(), is("'DESCRIBE' statements are not supported."));
  }

  @Test
  public void shouldThrowOnExplainStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("explain my_query_id;"));

    // Then:
    assertThat(e.getMessage(), is("'EXPLAIN' statements are not supported."));
  }

  @Test
  public void shouldThrowOnSelectStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("select * from my_stream emit changes;"));

    // Then:
    assertThat(e.getMessage(), is("'SELECT' statements are not supported."));
  }

  @Test
  public void shouldThrowOnPrintStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("print 'my_topic';"));

    // Then:
    assertThat(e.getMessage(), is("'PRINT' statements are not supported."));
  }

  @Test
  public void shouldThrowOnShowStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("show connectors;"));

    // Then:
    assertThat(e.getMessage(), is("'SHOW' statements are not supported."));
  }

  @Test
  public void shouldThrowOnListStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("list queries;"));

    // Then:
    assertThat(e.getMessage(), is("'LIST' statements are not supported."));
  }

  @Test
  public void shouldThrowOnRunScriptStatement() {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.parse("RUN SCRIPT 'my_script.sql';"));

    // Then:
    assertThat(e.getMessage(), is("'RUN SCRIPT' statements are not supported."));
  }
}
