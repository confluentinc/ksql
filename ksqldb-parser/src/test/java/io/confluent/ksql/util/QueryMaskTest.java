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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class QueryMaskTest {

  @Test
  public void shouldMaskConfigMap() {
    // Given
    final ImmutableMap<String, String> config = ImmutableMap.of(
        "connector.class", "someclass",
        "model", "somemode",
        "key", "somekey"
    );

    // When
    final Map<String, String> maskedConfig = QueryMask.getMaskedConnectConfig(config);

    // Then
    final ImmutableMap<String, String> expectedConfig = ImmutableMap.of(
        "connector.class", "someclass",
        "model", "'[string]'",
        "key", "'[string]'"
    );

    assertThat(maskedConfig, is(expectedConfig));
  }

  @Test
  public void shouldMaskIfNotExistSourceConnector() {
    // Given:
    final String query = "CREATE SOURCE CONNECTOR IF NOT EXISTS testconnector WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    `mode`='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE SOURCE CONNECTOR IF NOT EXISTS testconnector WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "`mode`='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskSourceConnector() {
    // Given:
    final String query = "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    `mode`='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "`mode`='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskSinkConnector() {
    // Given:
    final String query = "CREATE Sink CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
        final String expected = "CREATE SINK CONNECTOR `test-connector` WITH "
            + "(\"connector.class\"='PostgresSource', "
            + "'connection.url'='[string]', "
            + "\"mode\"='[string]', "
            + "\"topic.prefix\"='[string]', "
            + "\"table.whitelist\"='[string]', "
            + "\"key\"='[string]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskInvalidSinkConnector() {
    // Given:
    // Typo in "CONNECTOR" => "CONNECTO"
    final String query = "CREATE Sink CONNECTO `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    `mode`='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE Sink CONNECTO `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url'='[string]',\n"
        + "    `mode`='[string]',\n"
        + "    \"topic.prefix\"='[string]',\n"
        + "    \"table.whitelist\"='[string]',\n"
        + "    \"key\"='[string]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldNotMaskInvalidCreateStreamWithoutQuote() {
    // Given:
    // Typo in "WITH" => "WIT"
    final String query = "CREATE STREAM `stream` WIT ("
        + "    format = 'avro', \n"
        + "    kafka_topic = 'test_topic',"
        + "\"partitions\"=   3,\n"
        + ");";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    assertThat(maskedQuery, is(query));
  }

  @Test
  public void shouldNotMaskValidCreateStream() {
    // Given:
    final String query = "CREATE STREAM `stream` (id varchar) WITH (format = 'avro', kafka_topic = 'test_topic', partitions=3);";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    assertThat(maskedQuery, is(query));
  }

  @Test
  public void shouldMaskInvalidCreateStreamWithQuotes() {
    // Given:
    // Typo in "WITH" => "WIT"
    final String query = "CREATE STREAM `stream` (id varchar) WIT ('format' = 'avro', \"kafka_topic\" = 'test_topic', partitions=3);";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then: we are replacing key which is not in ALLOWED_KEYS if the statement is invalid and we do regex matching
    final String expected  = "CREATE STREAM `stream` (id varchar) WIT ('format'='[string]', \"kafka_topic\"='[string]', partitions=3);";
    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskValidCreateConnectorWithComment() {
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskMultipleValidStatements() {
    // Given:
    // Typo in "WITH" => "WIT"
    final String query = "CREATE SOURCE CONNECTOR test_connector WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    `mode`='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');\n"
        + "CREATE STREAM `stream` (id varchar) WITH ('format' = 'avro', \"kafka_topic\" = 'test_topic', partitions=3);";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE SOURCE CONNECTOR test_connector WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "`mode`='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');\n"
        + "CREATE STREAM `stream` (id varchar) WITH ('format' = 'avro', \"kafka_topic\" = 'test_topic', partitions=3);";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskMixedValidInvalidStatements() {
    // Given:
    // Typo in "WITH" => "WIT"
    final String query = "CREATE SOURCE CONNECTOR test_connector WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    `mode`='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');\n"
        + "CREATE STREAM `stream` (id varchar) WIT ('format' = 'avro', \"kafka_topic\" = 'test_topic', partitions=3);";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE SOURCE CONNECTOR test_connector WITH (    \"connector.class\" = 'PostgresSource', \n    "
        + "'connection.url'='[string]',\n    "
        + "`mode`='[string]',\n    "
        + "\"topic.prefix\"='[string]',\n    "
        + "\"table.whitelist\"='[string]',\n    "
        + "\"key\"='[string]');\n"
        + "CREATE STREAM `stream` (id varchar) WIT ('format'='[string]', \"kafka_topic\"='[string]', partitions=3);";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskInsertStatement() {
    // Given
    final String query = "--this is a comment. \n"
        + "INSERT INTO foo (KEY_COL, COL_A) VALUES (\"key\", \"A\");";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "INSERT INTO `FOO` (`KEY_COL`, `COL_A`) VALUES ('[value]', '[value]');";

    assertThat(maskedQuery, is(expected));
  }

  @Test
  public void shouldMaskFallbackInsertStatement() {
    // Given
    final String query = "--this is a comment. \n"
        + "INSERT INTO foo (KEY_COL, COL_A) VALUES"
        + "(\"key\", 0.125, '{something}', 1, C, 2.3E);";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    /// Then
    final String expected = "--this is a comment. \n"
        + "INSERT INTO foo (KEY_COL, COL_A) VALUES"
        + "('[value]','[value]','[value]','[value]','[value]','[value]');";


    assertThat(maskedQuery, is(expected));
  }
}
