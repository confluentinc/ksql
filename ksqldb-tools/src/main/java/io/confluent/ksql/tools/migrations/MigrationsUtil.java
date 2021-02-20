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

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import java.net.MalformedURLException;
import java.net.URL;

public final class MigrationsUtil {

  private MigrationsUtil() {
  }

  public static final String MIGRATIONS_DIR = "migrations";
  public static final String MIGRATIONS_CONFIG_FILE = "ksql-migrations.properties";

  public static Client getKsqlClient(final MigrationConfig config) throws MigrationException {
    final String ksqlServerUrl = config.getString(MigrationConfig.KSQL_SERVER_URL);
    return getKsqlClient(ksqlServerUrl);
  }

  public static Client getKsqlClient(final String ksqlServerUrl) throws MigrationException {
    final URL url;
    try {
      url = new URL(ksqlServerUrl);
    } catch (MalformedURLException e) {
      throw new MigrationException("Invalid ksql server URL: " + ksqlServerUrl);
    }

    final ClientOptions options = ClientOptions
        .create()
        .setHost(url.getHost())
        .setPort(url.getPort());

    return Client.create(options);
  }
}
