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
import org.junit.Test;

public class QueryMaskTest {

  @Test
  public void shouldMaskSourceConnector() {
    // Given:
    final String query = "CREATE SOURCE CONNECTOR `test-connector` WITH ("
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
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    // When
    final String maskedQuery = QueryMask.getMaskedStatement(query);

    // Then
    final String expected = "CREATE Sink CONNECTO `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url'='[string]',\n"
        + "    \"mode\"='[string]',\n"
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

    // Then
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
}
