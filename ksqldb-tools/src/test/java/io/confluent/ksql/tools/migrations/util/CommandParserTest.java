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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNull;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlConnectorStatement;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlInsertValues;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlPropertyCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlStatement;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Test;

public class CommandParserTest {

  @Test
  public void shouldParseInsertValuesStatement() {
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO FOO VALUES (55);");
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlInsertValues.class));
    assertThat(((SqlInsertValues) commands.get(0)).getSourceName(), is("FOO"));
    assertThat(((SqlInsertValues) commands.get(0)).getValues().size(), is(1));
    assertThat(toFieldType(((SqlInsertValues) commands.get(0)).getValues().get(0)), is(55));
  }

  @Test
  public void shouldParseInsertIntoStatement() {
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO FOO SELECT VALUES FROM BAR;");
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
  }

  @Test
  public void shouldParseSetUnsetStatements() {
    List<SqlCommand> commands = CommandParser.parse("SET 'foo.property'='bar';UNSET 'foo.property';");
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), instanceOf(SqlPropertyCommand.class));
    assertThat(commands.get(0).getCommand(), is("SET 'foo.property'='bar';"));
    assertThat(((SqlPropertyCommand) commands.get(0)).isSet(), is(true));
    assertThat(((SqlPropertyCommand) commands.get(0)).getProperty(), is("foo.property"));
    assertThat(((SqlPropertyCommand) commands.get(0)).getValue(), is("bar"));
    assertThat(commands.get(1), instanceOf(SqlPropertyCommand.class));
    assertThat(commands.get(1).getCommand(), is("UNSET 'foo.property';"));
    assertThat(((SqlPropertyCommand) commands.get(1)).isSet(), is(false));
    assertThat(((SqlPropertyCommand) commands.get(1)).getProperty(), is("foo.property"));
    assertNull(((SqlPropertyCommand) commands.get(1)).getValue());
  }

  @Test
  public void shouldParseMultipleStatements() {
    List<SqlCommand> commands = CommandParser.parse("INSERT INTO foo VALUES (32);INSERT INTO FOO_2 VALUES ('wow',3,'hello ''world''!');");
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
    List<SqlCommand> commands = CommandParser.parse("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');");
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), instanceOf(SqlStatement.class));
    assertThat(commands.get(0).getCommand(), is("CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');"));
  }

  @Test
  public void shouldParseSeveralCommands() {
    List<SqlCommand> commands = CommandParser.parse("CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);\n"
        + "INSERT INTO `riderLocations` (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);\n"
        + "INSERT INTO \"riderLocations\" (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);\n"
        + "INSERT INTO `values` (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);\n"
        + "INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);");
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
    final String createConnector = "CREATE SOURCE CONNECTOR `jdbc-connector` WITH(\n"
        + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
        + "    \"connection.url\"='jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";
    final String dropConnector = "DROP CONNECTOR `jdbc-connector`;";
    List<SqlCommand> commands = CommandParser.parse(createConnector + dropConnector);
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), instanceOf(SqlConnectorStatement.class));
    assertThat(commands.get(0).getCommand(), is(createConnector));
    assertThat(commands.get(1), instanceOf(SqlConnectorStatement.class));
    assertThat(commands.get(1).getCommand(), is(dropConnector));
  }

  @Test
  public void testSplit() {
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

    assertThat(commands.size(), is(5));
    assertThat(commands.get(0), is("CREATE STREAM valid_purchases AS\n  SELECT *\n  FROM purchases\n  WHERE cost > 0.00 AND quantity > 0;"));
    assertThat(commands.get(1), is("\nCREATE OR REPLACE STREAM valid_purchases AS\n  SELECT *\n  FROM purchases\n  WHERE quantity > 0;"));
    assertThat(commands.get(2), is("\nINSERT INTO purchases VALUES ('c''ow', -90);"));
    assertThat(commands.get(3), is("\nINSERT INTO purchases VALUES ('/*she*/ep',     80);"));
    assertThat(commands.get(4), is("\nINSERT INTO purchases VALUES ('pol/*ar;;be--ar*/;', 200000);"));
  }

  @Test
  public void shouldThowOnMalformedComment() throws Exception {
    // When:
    final MigrationException e = assertThrows(MigrationException.class,
        () -> CommandParser.splitSql("/* Comment "));
    // Then:
    assertThat(e.getMessage(), is("Invalid sql - failed to find closing token '*/'"));
  }
}
