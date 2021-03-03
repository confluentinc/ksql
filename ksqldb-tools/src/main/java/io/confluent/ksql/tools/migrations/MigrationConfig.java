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

import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.tools.migrations.util.ServerVersionUtil;
import java.io.File;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public final class MigrationConfig extends AbstractConfig {

  public static final String KSQL_SERVER_URL = "ksql.server.url";

  public static final String KSQL_MIGRATIONS_STREAM_NAME = "ksql.migrations.stream.name";
  public static final String KSQL_MIGRATIONS_STREAM_NAME_DEFAULT = "MIGRATION_EVENTS";
  public static final String KSQL_MIGRATIONS_TABLE_NAME = "ksql.migrations.table.name";
  public static final String KSQL_MIGRATIONS_TABLE_NAME_DEFAULT = "MIGRATION_SCHEMA_VERSIONS";
  public static final String KSQL_MIGRATIONS_STREAM_TOPIC_NAME =
      "ksql.migrations.stream.topic.name";
  public static final String KSQL_MIGRATIONS_TABLE_TOPIC_NAME = "ksql.migrations.table.topic.name";
  public static final String KSQL_MIGRATIONS_TOPIC_REPLICAS = "ksql.migrations.topic.replicas";
  public static final int KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT = 1;

  public static MigrationConfig load(final String configFile) {
    final Map<String, String> configsMap =
        PropertiesUtil.loadProperties(new File(configFile));
    return new MigrationConfig(configsMap, getServiceId(configsMap));
  }

  private MigrationConfig(final Map<String, String> configs, final String id) {
    super(new ConfigDef()
        .define(
            KSQL_SERVER_URL,
            Type.STRING,
            "",
            Importance.HIGH,
            "The URL for the KSQL server"
        )
        .define(
            KSQL_MIGRATIONS_STREAM_NAME,
            Type.STRING,
            KSQL_MIGRATIONS_STREAM_NAME_DEFAULT,
            Importance.MEDIUM,
            "The name of the migration stream. It defaults to "
                + KSQL_MIGRATIONS_STREAM_NAME_DEFAULT
        ).define(
            KSQL_MIGRATIONS_TABLE_NAME,
            Type.STRING,
            KSQL_MIGRATIONS_TABLE_NAME_DEFAULT,
            Importance.MEDIUM,
            "The name of the migration table. It defaults to "
                + KSQL_MIGRATIONS_TABLE_NAME_DEFAULT
        ).define(
            KSQL_MIGRATIONS_STREAM_TOPIC_NAME,
            Type.STRING,
            id + "ksql_" + configs
                .getOrDefault(KSQL_MIGRATIONS_STREAM_NAME, KSQL_MIGRATIONS_STREAM_NAME_DEFAULT),
            Importance.MEDIUM,
            "The name of the migration stream topic. It defaults to "
                + "'<ksql_service_id>ksql_<migrations_stream_name>'"
        ).define(
            KSQL_MIGRATIONS_TABLE_TOPIC_NAME,
            Type.STRING,
            id + "ksql_" + configs
                .getOrDefault(KSQL_MIGRATIONS_TABLE_NAME, KSQL_MIGRATIONS_TABLE_NAME_DEFAULT),
            Importance.MEDIUM,
            "The name of the migration table topic. It defaults to "
                + "'<ksql_service_id>ksql_<migrations_table_name>'"
        ).define(
            KSQL_MIGRATIONS_TOPIC_REPLICAS,
            Type.INT,
            KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT,
            Importance.MEDIUM,
            "The number of replicas for the migration stream topic. It defaults to "
                + KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT
        ), configs);
  }

  private static String getServiceId(final Map<String, String> configs) throws MigrationException {
    final String ksqlServerUrl = configs.get(MigrationConfig.KSQL_SERVER_URL);
    if (ksqlServerUrl == null) {
      throw new MigrationException("Missing required property: " + MigrationConfig.KSQL_SERVER_URL);
    }

    return ServerVersionUtil.getServerInfo(ksqlServerUrl).getKsqlServiceId();
  }
}
