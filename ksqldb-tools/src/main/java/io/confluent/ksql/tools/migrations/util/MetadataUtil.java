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

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MetadataUtil {
  private MetadataUtil() {
  }

  public static String getCurrentVersion(final MigrationConfig config) {
    final Client client = MigrationsUtil.getKsqlClient(config);
    final String migrationTableName = config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    BatchedQueryResult result = client.executeQuery(
        "SELECT VERSION FROM " + migrationTableName + " WHERE version_key = 'CURRENT';");
    try {
      List<Row> resultRows = result.get();
      if (resultRows.size() == 0) {
        return "0";
      }
      return resultRows.get(0).getString("VERSION");
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(
          String.format("Could not query %s: %s", migrationTableName, e.getMessage()));
    } finally {
      client.close();
    }
  }
}
