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

import static io.confluent.ksql.tools.migrations.util.MetadataUtil.EMPTY_ERROR_REASON;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public final class MigrationVersionInfo {

  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
  private static final String EMPTY_MIGRATION_TIMESTAMP = "N/A";

  private final int version;
  private final String expectedHash;
  private final String prevVersion;
  private final MigrationState state;
  private final String name;
  private final String startedOn;
  private final String completedOn;
  private final String errorReason;

  public static MigrationVersionInfo fromResultRow(final Row row) {
    return new MigrationVersionInfo(
        Integer.parseInt(row.getString(1)),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getString(7),
        row.getString(8)
    );
  }

  public static MigrationVersionInfo pendingMigration(final int version, final String name) {
    return new MigrationVersionInfo(
        version,
        "N/A",
        "N/A",
        MigrationState.PENDING.toString(),
        name,
        EMPTY_MIGRATION_TIMESTAMP,
        EMPTY_MIGRATION_TIMESTAMP,
        EMPTY_ERROR_REASON
    );
  }

  @VisibleForTesting
  MigrationVersionInfo(
      final int version,
      final String expectedHash,
      final String prevVersion,
      final String state,
      final String name,
      final String startedOn,
      final String completedOn,
      final String errorReason
  ) {
    this.version = version;
    this.expectedHash = Objects.requireNonNull(expectedHash, "expectedHash");
    this.prevVersion = Objects.requireNonNull(prevVersion, "prevVersion");
    this.state = MigrationState.valueOf(Objects.requireNonNull(state, "state"));
    this.name = Objects.requireNonNull(name, "name");
    this.errorReason = Objects.requireNonNull(errorReason, "errorReason");

    Objects.requireNonNull(startedOn, "startedOn");
    Objects.requireNonNull(completedOn, "completedOn");
    this.startedOn = formatTimestamp(startedOn);
    this.completedOn = formatTimestamp(completedOn);
  }

  public int getVersion() {
    return version;
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

  public String getName() {
    return name;
  }

  public String getStartedOn() {
    return startedOn;
  }

  public String getCompletedOn() {
    return completedOn;
  }

  public String getErrorReason() {
    return errorReason;
  }

  private static String formatTimestamp(final String epochTime) {
    if (epochTime.isEmpty() || epochTime.equals(EMPTY_MIGRATION_TIMESTAMP)) {
      return epochTime;
    }

    return new SimpleDateFormat(DATE_FORMAT).format(new Date(Long.parseLong(epochTime)));
  }
}
