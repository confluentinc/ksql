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

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServerVersionUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerVersionUtil.class);

  private ServerVersionUtil() {
  }

  public static ServerInfo getServerInfo(final Client ksqlClient, final String ksqlServerUrl) {
    final CompletableFuture<ServerInfo> response = ksqlClient.serverInfo();

    try {
      return response.get();
    } catch (InterruptedException e) {
      throw new MigrationException("Interrupted while attempting to connect to "
          + ksqlServerUrl + "/info");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IllegalStateException) {
        throw new MigrationException(e.getCause().getMessage()
            + "\nPlease ensure that " + ksqlServerUrl + " is an active ksqlDB server and that the "
            + "version of the migration tool is compatible with the version of the ksqlDB server.");
      }
      throw new MigrationException("Failed to query " + ksqlServerUrl + "/info: " + e.getMessage());
    }
  }

  public static boolean isSupportedVersion(final String ksqlServerVersion) {
    final KsqlServerVersion version;
    try {
      version = new KsqlServerVersion(ksqlServerVersion);
    } catch (IllegalArgumentException e) {
      throw new MigrationException("Could not parse ksqlDB server version to "
          + "verify compatibility. Version: " + ksqlServerVersion);
    }

    return version.isAtLeast(6, 0, 0, 10);
  }

  public static boolean versionSupportsMultiKeyPullQuery(final String ksqlServerVersion) {
    final KsqlServerVersion version;
    try {
      version = new KsqlServerVersion(ksqlServerVersion);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Could not parse ksqlDB server version to verify whether multi-key pull queries "
          + "are supported. Falling back to single-key pull queries only.");
      return false;
    }

    return version.isAtLeast(6, 1, 0, 14);
  }

  public static boolean serverVersionCompatible(
      final Client ksqlClient,
      final MigrationConfig config
  ) {
    final String ksqlServerUrl = config.getString(MigrationConfig.KSQL_SERVER_URL);
    final ServerInfo serverInfo;
    try {
      serverInfo = getServerInfo(ksqlClient, ksqlServerUrl);
    } catch (MigrationException e) {
      LOGGER.error("Failed to get server info to verify version compatibility: {}", e.getMessage());
      return false;
    }

    final String serverVersion = serverInfo.getServerVersion();
    try {
      return isSupportedVersion(serverVersion);
    } catch (MigrationException e) {
      LOGGER.warn(e.getMessage() + ". Proceeding anyway.");
      return true;
    }
  }

  private static class KsqlServerVersion {

    private static final Pattern VERSION_PATTERN = Pattern.compile("v?([0-9]+)\\.([0-9]+)\\..*");

    private enum VersionType {
      CONFLUENT_PLATFORM,
      KSQLDB_STANDALONE
    }

    private final VersionType versionType;
    private final int majorVersion;
    private final int minorVersion;

    KsqlServerVersion(final String version) {
      final Matcher matcher = VERSION_PATTERN.matcher(version);
      if (!matcher.find()) {
        throw new IllegalArgumentException("Unexpected ksqlDB server version: " + version);
      }

      majorVersion = Integer.parseInt(matcher.group(1));
      minorVersion = Integer.parseInt(matcher.group(2));
      versionType = majorVersion < 4
          ? VersionType.KSQLDB_STANDALONE
          : VersionType.CONFLUENT_PLATFORM;
    }

    boolean isAtLeast(
        final int cpMajor,
        final int cpMinor,
        final int standaloneMajor,
        final int standaloneMinor
    ) {
      return versionType == VersionType.CONFLUENT_PLATFORM
          ? isAtLeastVersion(cpMajor, cpMinor)
          : isAtLeastVersion(standaloneMajor, standaloneMinor);
    }

    private boolean isAtLeastVersion(final int major, final int minor) {
      if (majorVersion > major) {
        return true;
      } else if (majorVersion < major) {
        return false;
      } else {
        return minorVersion >= minor;
      }
    }
  }
}
