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

import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.getServerInfo;
import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.versionSupportsMultiKeyPullQuery;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class MetadataUtil {

  public static final String NONE_VERSION = "<none>";
  public static final String CURRENT_VERSION_KEY = "CURRENT";
  public static final String EMPTY_ERROR_REASON = "N/A";
  private static final List<String> KEYS = ImmutableList.of(
      "VERSION_KEY", "VERSION", "NAME", "STATE",
      "CHECKSUM", "STARTED_ON", "COMPLETED_ON", "PREVIOUS", "ERROR_REASON"
  );

  public enum MigrationState {
    PENDING,
    RUNNING,
    MIGRATED,
    ERROR
  }

  private MetadataUtil() {
  }

  public static String getCurrentVersion(final MigrationConfig config, final Client client) {
    final String migrationTableName = config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final BatchedQueryResult result = client.executeQuery(
        "SELECT VERSION FROM " + migrationTableName + " WHERE version_key = '"
            + CURRENT_VERSION_KEY + "';");
    try {
      final List<Row> resultRows = result.get();
      if (resultRows.size() == 0) {
        return NONE_VERSION;
      }
      return resultRows.get(0).getString("VERSION");
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(
          String.format("Could not query %s: %s", migrationTableName, e.getMessage()));
    }
  }

  public static CompletableFuture<Void> writeRow(
      final MigrationConfig config,
      final Client client,
      final String versionKey,
      final String state,
      final String startOn,
      final String completedOn,
      final MigrationFile migration,
      final String previous,
      final String checksum,
      final Optional<String> errorReason
  ) {
    final String migrationStreamName =
        config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME);
    final List<String> values = ImmutableList.of(
        versionKey,
        Integer.toString(migration.getVersion()),
        migration.getName(),
        state,
        checksum,
        startOn,
        completedOn,
        previous,
        errorReason.orElse(EMPTY_ERROR_REASON)
    );
    return client.insertInto(
        migrationStreamName,
        KsqlObject.fromArray(KEYS, new KsqlArray(values))
    );
  }

  public static String getLatestMigratedVersion(
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String currentVersion = MetadataUtil.getCurrentVersion(config, ksqlClient);
    if (currentVersion.equals(MetadataUtil.NONE_VERSION)) {
      return currentVersion;
    }

    final MigrationVersionInfo currentVersionInfo =
        getInfoForVersion(currentVersion, config, ksqlClient);
    if (currentVersionInfo.getState() == MigrationState.MIGRATED) {
      return currentVersion;
    }

    if (currentVersionInfo.getPrevVersion().equals(MetadataUtil.NONE_VERSION)) {
      return MetadataUtil.NONE_VERSION;
    }

    final MigrationVersionInfo prevVersionInfo = getInfoForVersion(
        currentVersionInfo.getPrevVersion(),
        config,
        ksqlClient
    );
    validateVersionIsMigrated(currentVersionInfo.getPrevVersion(), prevVersionInfo, currentVersion);

    return currentVersionInfo.getPrevVersion();
  }

  public static void validateVersionIsMigrated(
      final String version,
      final MigrationVersionInfo versionInfo,
      final String nextVersion
  ) {
    if (versionInfo.getState() != MigrationState.MIGRATED) {
      throw new MigrationException(String.format(
          "Discovered version with previous version that does not have status %s. "
              + "Version: %s. Previous version: %s. Previous version status: %s",
          MigrationState.MIGRATED,
          nextVersion,
          version,
          versionInfo.getState()
      ));
    }
  }

  public static MigrationVersionInfo getInfoForVersion(
      final String version,
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final Optional<MigrationVersionInfo> maybeInfo =
        getOptionalInfoForVersion(version, config, ksqlClient);
    return maybeInfo.orElseThrow(() -> new MigrationException(
        "Failed to query state for migration with version " + version
            + ": no such migration is present in the migrations metadata table"));
  }

  public static Optional<MigrationVersionInfo> getOptionalInfoForVersion(
      final String version,
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String migrationTableName = config
        .getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final BatchedQueryResult result = ksqlClient.executeQuery(
        "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
            + "FROM " + migrationTableName + " WHERE version_key = '" + version + "';");

    final Row resultRow;
    try {
      final List<Row> resultRows = result.get();
      if (resultRows.size() == 0) {
        return Optional.empty();
      }
      resultRow = resultRows.get(0);
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format(
          "Failed to query state for migration with version %s: %s", version, e.getMessage()));
    }

    return Optional.of(MigrationVersionInfo.fromResultRow(resultRow));
  }

  public static Map<Integer, Optional<MigrationVersionInfo>> getOptionalInfoForVersions(
      final List<Integer> versions,
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    if (serverSupportsMultiKeyPullQuery(ksqlClient, config)) {
      // issue a single, multi-key pull query
      final String migrationTableName = config
          .getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
      final BatchedQueryResult result = ksqlClient.executeQuery(
          "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
              + "FROM " + migrationTableName + " WHERE version_key IN ('"
              + versions.stream().map(String::valueOf).collect(Collectors.joining("', '"))
              + "');");

      final Map<Integer, MigrationVersionInfo> resultSet;
      try {
        resultSet = result.get().stream()
            .map(MigrationVersionInfo::fromResultRow)
            .collect(Collectors.toMap(MigrationVersionInfo::getVersion, vInfo -> vInfo));
      } catch (InterruptedException | ExecutionException e) {
        throw new MigrationException(String.format(
            "Failed to query state for migration with versions %s: %s", versions, e.getMessage()));
      }

      return versions.stream()
          .collect(Collectors.toMap(v -> v, v -> Optional.ofNullable(resultSet.get(v))));
    } else {
      // issue multiple, single-key pull queries
      return versions.stream()
          .collect(Collectors.toMap(
              v -> v,
              v -> getOptionalInfoForVersion(String.valueOf(v), config, ksqlClient)));
    }
  }

  private static boolean serverSupportsMultiKeyPullQuery(
      final Client ksqlClient,
      final MigrationConfig config
  ) {
    final String ksqlServerUrl = config.getString(MigrationConfig.KSQL_SERVER_URL);
    final ServerInfo serverInfo = getServerInfo(ksqlClient, ksqlServerUrl);
    return versionSupportsMultiKeyPullQuery(serverInfo.getServerVersion());
  }
}
