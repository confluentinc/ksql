/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.server.resources.CommandTopicCorruptionException;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backup service that replays the KSQL command_topic to a local file. A new backup file is
 * created whenever a new command does not match the actual backup file. Previously replayed
 * messages read up to this new command will be written to the new file. This ensures a new
 * complete backup of the command_topic is created.
 */
public class CommandTopicBackupImpl implements CommandTopicBackup {

  private static final Logger LOG = LoggerFactory.getLogger(CommandTopicBackupImpl.class);
  private static final String PREFIX = "backup_";

  private final File backupLocation;
  private final String topicName;
  private final Supplier<Long> ticker;

  private BackupReplayFile replayFile;
  private List<Pair<byte[], byte[]>> latestReplay;
  private int latestReplayIdx;
  private boolean corruptionDetected;

  public CommandTopicBackupImpl(
      final String location,
      final String topicName
  ) {
    this(location, topicName, System::currentTimeMillis);
  }

  @VisibleForTesting
  CommandTopicBackupImpl(
      final String location,
      final String topicName,
      final Supplier<Long> ticker
  ) {
    this.backupLocation = new File(Objects.requireNonNull(location, "location"));
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.ticker = Objects.requireNonNull(ticker, "ticker");

    ensureDirectoryExists(backupLocation);
  }

  @Override
  public void initialize() {
    replayFile = openOrCreateReplayFile();

    try {
      latestReplay = replayFile.readRecords();
    } catch (final IOException e) {
      LOG.warn("Failed to read the latest backup from {}. Continue with a new file. Error = {}",
          replayFile.getPath(), e.getMessage());

      replayFile = newReplayFile();
      latestReplay = Collections.emptyList();
    }

    latestReplayIdx = 0;
    corruptionDetected = false;
    LOG.info("Command topic will be backup on file: {}", replayFile.getPath());
  }

  @Override
  public void close() {
    try {
      replayFile.close();
    } catch (final IOException e) {
      LOG.warn("Failed closing the backup file {}. Error = {}",
          replayFile.getPath(), e.getMessage());
    }
  }

  @VisibleForTesting
  BackupReplayFile getReplayFile() {
    return replayFile;
  }

  private boolean isRestoring() {
    return latestReplayIdx < latestReplay.size();
  }

  private boolean isRecordInLatestReplay(final ConsumerRecord<byte[], byte[]> record) {
    final Pair<byte[], byte[]> latestReplayRecord = latestReplay.get(latestReplayIdx);

    if (Arrays.equals(record.key(), latestReplayRecord.getLeft())
        && Arrays.equals(record.value(), latestReplayRecord.getRight())) {
      latestReplayIdx++;
      return true;
    }

    return false;
  }

  @Override
  public void writeRecord(final ConsumerRecord<byte[], byte[]> record) {
    if (corruptionDetected) {
      throw new CommandTopicCorruptionException(
          "Failed to write record due to out of sync command topic and backup file: " + record);
    }

    if (isRestoring()) {
      if (isRecordInLatestReplay(record)) {
        // Ignore backup because record was already replayed
        return;
      } else {
        corruptionDetected = true;
        throw new CommandTopicCorruptionException(
            "Failed to write record due to out of sync command topic and backup file: " + record);
      }
    } else if (latestReplay.size() > 0) {
      // clear latest replay from memory
      latestReplay.clear();
    }

    try {
      replayFile.write(record);
    } catch (final IOException e) {
      LOG.warn("Failed to write to file {}. The command topic backup is not complete. "
              + "Make sure the file exists and has permissions to write. KSQL must be restarted "
              + "afterwards to complete the backup process. Error = {}",
          replayFile.getPath(), e.getMessage());
    }
  }

  @Override
  public boolean commandTopicCorruption() {
    return corruptionDetected;
  }

  @VisibleForTesting
  BackupReplayFile openOrCreateReplayFile() {
    return latestReplayFile()
        .orElseGet(this::newReplayFile);
  }

  private BackupReplayFile newReplayFile() {
    return BackupReplayFile.writable(Paths.get(
        backupLocation.getAbsolutePath(),
        String.format("%s%s_%s", PREFIX, topicName, ticker.get())
    ).toFile());
  }

  private Optional<BackupReplayFile> latestReplayFile() {
    final String prefixFilename = String.format("%s%s_", PREFIX, topicName);
    final File[] files = backupLocation.listFiles(
        (f, name) -> name.toLowerCase().startsWith(prefixFilename));

    File latestBakFile = null;
    if (files != null) {
      long latestTs = 0;
      for (final File bakFile : files) {
        final String bakTimestamp = bakFile.getName().substring(prefixFilename.length());

        try {
          final long ts = Long.parseLong(bakTimestamp);
          if (ts > latestTs) {
            latestTs = ts;
            latestBakFile = bakFile;
          }
        } catch (final NumberFormatException e) {
          LOG.warn(
              "Invalid timestamp '{}' found in backup replay file (file ignored): {}",
              bakTimestamp, bakFile.getName());
        }
      }
    }

    return Optional.ofNullable(latestBakFile)
        .map(BackupReplayFile::writable);
  }

  private static void ensureDirectoryExists(final File backupsDir) {
    if (!backupsDir.exists()) {
      if (!backupsDir.mkdirs()) {
        throw new KsqlServerException("Couldn't create the backups directory: "
            + backupsDir.getPath()
            + "\n Make sure the directory exists and is readable/writable for KSQL server "
            + "\n or its parent directory is readable/writable by KSQL server"
            + "\n or change it to a readable/writable directory by setting '"
            + KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION
            + "' config in the properties file."
        );
      }

      try {
        Files.setPosixFilePermissions(backupsDir.toPath(),
            PosixFilePermissions.fromString("rwx------"));
      } catch (final IOException e) {
        throw new KsqlServerException(String.format(
            "Couldn't set POSIX permissions on the backups directory: %s. Error = %s",
            backupsDir.getPath(), e.getMessage()));
      }
    }

    if (!backupsDir.isDirectory()) {
      throw new KsqlServerException(backupsDir.getPath()
          + " is not a directory."
          + "\n Make sure the directory exists and is readable/writable for KSQL server "
          + "\n or its parent directory is readable/writable by KSQL server"
          + "\n or change it to a readable/writable directory by setting '"
          + KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION
          + "' config in the properties file."
      );
    }

    if (!backupsDir.canWrite() || !backupsDir.canRead() || !backupsDir.canExecute()) {
      throw new KsqlServerException("The backups directory is not readable/writable "
          + "for KSQL server: "
          + backupsDir.getPath()
          + "\n Make sure the directory exists and is readable/writable for KSQL server "
          + "\n or change it to a readable/writable directory by setting '"
          + KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION
          + "' config in the properties file."
      );
    }
  }
}
