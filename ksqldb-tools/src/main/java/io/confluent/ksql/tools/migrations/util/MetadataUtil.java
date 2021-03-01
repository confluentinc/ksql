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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.Migration;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class MetadataUtil {

  public static final String NONE_VERSION = "<none>";
  public static final String CURRENT_VERSION_KEY = "CURRENT";
  private static final List<String> KEYS = ImmutableList.of(
      "VERSION_KEY", "VERSION", "NAME", "STATE",
      "CHECKSUM", "STARTED_ON", "COMPLETED_ON", "PREVIOUS"
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
      final Migration migration,
      final String previous,
      final String checksum
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
        previous
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

    final VersionInfo currentVersionInfo = getInfoForVersion(currentVersion, config, ksqlClient);
    if (currentVersionInfo.state == MigrationState.MIGRATED) {
      return currentVersion;
    }

    if (currentVersionInfo.prevVersion.equals(MetadataUtil.NONE_VERSION)) {
      return MetadataUtil.NONE_VERSION;
    }

    final VersionInfo prevVersionInfo = getInfoForVersion(
        currentVersionInfo.prevVersion,
        config,
        ksqlClient
    );
    validateVersionIsMigrated(currentVersionInfo.prevVersion, prevVersionInfo, currentVersion);

    return currentVersionInfo.prevVersion;
  }

  public static void validateVersionIsMigrated(
      final String version,
      final VersionInfo versionInfo,
      final String nextVersion
  ) {
    if (versionInfo.state != MigrationState.MIGRATED) {
      throw new MigrationException(String.format(
          "Discovered version with previous version that does not have status %s. "
              + "Version: %s. Previous version: %s. Previous version status: %s",
          MigrationState.MIGRATED,
          nextVersion,
          version,
          versionInfo.state
      ));
    }
  }

  public static VersionInfo getInfoForVersion(
      final String version,
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String migrationTableName = config
        .getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final BatchedQueryResult result = ksqlClient.executeQuery(
        "SELECT checksum, previous, state FROM " + migrationTableName
            + " WHERE version_key = '" + version + "';");

    final String expectedHash;
    final String prevVersion;
    final String state;
    try {
      final List<Row> resultRows = result.get();
      if (resultRows.size() == 0) {
        throw new MigrationException(
            "Failed to query state for migration with version " + version
                + ": no such migration is present in the migrations metadata table");
      }
      expectedHash = resultRows.get(0).getString(1);
      prevVersion = resultRows.get(0).getString(2);
      state = resultRows.get(0).getString(3);
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format(
          "Failed to query state for migration with version %s: %s", version, e.getMessage()));
    }

    return new VersionInfo(expectedHash, prevVersion, state);
  }

  public static class VersionInfo {
    private final String expectedHash;
    private final String prevVersion;
    private final MigrationState state;

    VersionInfo(final String expectedHash, final String prevVersion, final String state) {
      this.expectedHash = Objects.requireNonNull(expectedHash, "expectedHash");
      this.prevVersion = Objects.requireNonNull(prevVersion, "prevVersion");
      this.state = MigrationState.valueOf(Objects.requireNonNull(state, "state"));
    }

    public String getExpectedHash() {
      return expectedHash;
    }

    public String getPrevVersion() {
      return prevVersion;
    }

    public MigrationState getState() {
      return state;
    }
  }
}
