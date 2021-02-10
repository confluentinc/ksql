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

package io.confluent.ksql.tools.migrations.commands;

import com.github.rvesse.airline.annotations.Command;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import java.io.File;
import java.util.Collections;
import java.util.Optional;

@Command(
    name = "initialize",
    description = "Initializes the schema metadata (stream and table)."
)
public class InitializeMigrationCommand extends BaseCommand {

  private String createEventStream(final String name, final String topic, final String replicas) {
    return "CREATE STREAM " + name + " (\n"
        + "  version_key  STRING KEY,\n"
        + "  version      STRING,\n"
        + "  name         STRING,\n"
        + "  state        STRING,  \n"
        + "  checksum     STRING,\n"
        + "  started_on   STRING,\n"
        + "  completed_on STRING,\n"
        + "  previous     STRING\n"
        + ") WITH (  \n"
        + "  KAFKA_TOPIC='" + topic + "',\n"
        + "  VALUE_FORMAT='JSON',\n"
        + "  PARTITIONS=1,\n"
        + "  REPLICAS= " + replicas + " \n"
        + ");\n";
  }

  private String createVersionTable(final String name, final String topic) {
    return "CREATE TABLE " + name + "\n"
        + "  WITH (\n"
        + "    KAFKA_TOPIC='" + topic + "'\n"
        + "  )\n"
        + "  AS SELECT \n"
        + "    version_key, \n"
        + "    latest_by_offset(version) as version, \n"
        + "    latest_by_offset(name) AS name, \n"
        + "    latest_by_offset(state) AS state,     \n"
        + "    latest_by_offset(checksum) AS checksum, \n"
        + "    latest_by_offset(started_on) AS started_on, \n"
        + "    latest_by_offset(completed_on) AS completed_on, \n"
        + "    latest_by_offset(previous) AS previous\n"
        + "  FROM migration_events \n"
        + "  GROUP BY version_key;\n";
  }

  @Override
  public void run() {
    final MigrationConfig properties = new MigrationConfig(
        PropertiesUtil.loadProperties(new File("ksql-migrations.properties")));
    final String eventStreamCommand = createEventStream(
        properties.getKsqlMigrationsStreamName(),
        properties.getKsqlMigrationsStreamTopicName(),
        properties.getKsqlMigrationsTopicReplicas()
    );
    final String versionTableCommand = createVersionTable(
        properties.getKsqlMigrationsTableName(),
        properties.getKsqlMigrationsTableTopicName()
    );
    if (dryRun) {
      System.out.println(eventStreamCommand);
      System.out.println(versionTableCommand);
    } else {
      final KsqlRestClient client = KsqlRestClient.create(
          properties.getKsqlServerUrl(),
          Collections.EMPTY_MAP,
          Collections.EMPTY_MAP,
          Optional.empty()
      );
      final RestResponse<KsqlEntityList> streamResponse =
          client.makeKsqlRequest(eventStreamCommand);
      final RestResponse<KsqlEntityList> tableResponse =
          client.makeKsqlRequest(versionTableCommand);

      if (streamResponse.isSuccessful()) {
        System.out.println("Successfully created migration stream");
      } else {
        System.out.println("Failed to create migration stream: "
            + streamResponse.getErrorMessage());
      }

      if (tableResponse.isSuccessful()) {
        System.out.println("Successfully created migration table");
      } else {
        System.out.println("Failed to create migration table: "
            + tableResponse.getErrorMessage());
      }
    }
  }
}
