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

import java.util.Map;

public class MigrationConfig  {

  public static final String KSQL_MIGRATIONS_STREAM_NAME = "ksql.migrations.stream.name";
  public static final String KSQL_MIGRATIONS_TABLE_NAME = "ksql.migrations.table.name";
  public static final String KSQL_MIGRATIONS_STREAM_TOPIC_NAME = "ksql.migrations.stream.topic.name";
  public static final String KSQL_MIGRATIONS_TABLE_TOPIC_NAME = "ksql.migrations.table.topic.name";
  public static final String KSQL_MIGRATIONS_TOPIC_REPLICAS = "ksql.migrations.topic.replicas";
  public static final String KSQL_SERVER_URL = "ksql.server.url";

  private static Map<String, String> configs;

  public MigrationConfig(final Map<String, String> configs) {
    this.configs = configs;
  }

  public String getKsqlMigrationsStreamName() {
    return configs.getOrDefault(KSQL_MIGRATIONS_STREAM_NAME, "migration_events");
  }

  public String getKsqlMigrationsTableName() {
    return configs.getOrDefault(KSQL_MIGRATIONS_TABLE_NAME, "schema_version");
  }

  public String getKsqlMigrationsStreamTopicName() {
    return configs.getOrDefault(
        KSQL_MIGRATIONS_STREAM_TOPIC_NAME, "default_ksql_migration_events");
  }

  public String getKsqlMigrationsTableTopicName() {
    return configs.getOrDefault(
        KSQL_MIGRATIONS_TABLE_TOPIC_NAME, "default_ksql_schema_version");
  }

  public String getKsqlMigrationsTopicReplicas() {
    return configs.getOrDefault(
        KSQL_MIGRATIONS_TOPIC_REPLICAS, "1");
  }

  public String getKsqlServerUrl() {
    return configs.getOrDefault(
        KSQL_SERVER_URL, "http://localhost:8088");
  }
}
