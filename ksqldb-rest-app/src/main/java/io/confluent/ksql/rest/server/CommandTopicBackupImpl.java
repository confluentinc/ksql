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
import com.google.common.base.Ticker;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
  private static final Ticker CURRENT_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };
  private static final String PREFIX = "backup_";

  private final File backupLocation;
  private final String topicName;
  private final Ticker ticker;

  private BackupReplayFile replayFile;
  private List<Pair<CommandId, Command>> latestReplay;
  private int latestReplayIdx;

  public CommandTopicBackupImpl(final String location, final String topicName) {
    this(location, topicName, CURRENT_MILLIS_TICKER);
  }

  public CommandTopicBackupImpl(
      final String location,
      final String topicName,
      final Ticker ticker
  ) {
    final File dir = new File(Objects.requireNonNull(location, "location"));
    ensureDirectoryExists(dir);

    this.backupLocation = dir;
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.ticker = Objects.requireNonNull(ticker, "ticker");
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

  private boolean isRecordInLatestReplay(final ConsumerRecord<CommandId, Command> record) {
    final Pair<CommandId, Command> latestReplayRecord = latestReplay.get(latestReplayIdx);
    if (record.key().equals(latestReplayRecord.left)
        && record.value().equals(latestReplayRecord.right)) {
      latestReplayIdx++;
      return true;
    }

    return false;
  }

  @Override
  public void writeRecord(final ConsumerRecord<byte[], byte[]> record) {
    final ConsumerRecord<CommandId, Command> deserializedRecord;
    try {
      deserializedRecord = new ConsumerRecord<>(
          record.topic(),
          record.partition(),
          record.offset(),
          InternalTopicSerdes.deserializer(CommandId.class)
              .deserialize(record.topic(), record.key()),
          InternalTopicSerdes.deserializer(Command.class)
              .deserialize(record.topic(), record.value())
      );
    } catch (Exception e) {
      LOG.error("Failed to deserialize command topic record when backing it up: {}:{}",
          record.key(), record.value());
      return;
    }
    writeCommandToBackup(deserializedRecord);
  }

  void writeCommandToBackup(final ConsumerRecord<CommandId, Command> record) {
    if (isRestoring()) {
      if (isRecordInLatestReplay(record)) {
        // Ignore backup because record was already replayed
        return;
      } else {
        LOG.info("Previous command topic backup does not match the new command topic data. "
            + "A new backup file will be created.");
        createNewBackupFile();
        latestReplay.clear();
        LOG.info("New backup file created: {}", replayFile.getPath());
      }
    } else if (latestReplay.size() > 0) {
      // clear latest replay from memory
      latestReplay.clear();
    }

    try {
      replayFile.write(record.key(), record.value());
    } catch (final IOException e) {
      LOG.warn("Failed to write to file {}. The command topic backup is not complete. "
              + "Make sure the file exists and has permissions to write. KSQL must be restarted "
              + "afterwards to complete the backup process. Error = {}",
          replayFile.getPath(), e.getMessage());
    }
  }

  private void createNewBackupFile() {
    try {
      replayFile.close();
    } catch (IOException e) {
      LOG.warn("Couldn't close the current backup file {}. Error = {}",
          replayFile.getPath(), e.getMessage());
    }

    replayFile = newReplayFile();

    if (latestReplay.size() > 0 && latestReplayIdx > 0) {
      try {
        replayFile.write(latestReplay.subList(0, latestReplayIdx));
      } catch (final IOException e) {
        LOG.warn("Couldn't write the latest replayed commands to the new backup file {}. "
                + "Make sure the file exists and has permissions to write. "
                + "KSQL must be restarted afterwards to complete the backup process. Error = {}",
            replayFile.getPath(), e.getMessage());
      }
    }
  }

  @VisibleForTesting
  BackupReplayFile openOrCreateReplayFile() {
    final Optional<BackupReplayFile> latestFile = latestReplayFile();
    if (latestFile.isPresent()) {
      return latestFile.get();
    }

    return newReplayFile();
  }

  private BackupReplayFile newReplayFile() {
    return new BackupReplayFile(Paths.get(
        backupLocation.getAbsolutePath(),
        String.format("%s%s_%s", PREFIX, topicName, ticker.read())
    ).toFile());
  }

  private Optional<BackupReplayFile> latestReplayFile() {
    final String prefixFilename = String.format("%s%s_", PREFIX, topicName);
    final File[] files = backupLocation.listFiles(
        (f, name) -> name.toLowerCase().startsWith(prefixFilename));

    File latestBakFile = null;
    if (files != null) {
      long latestTs = 0;
      for (int i = 0; i < files.length; i++) {
        final File bakFile = files[i];
        final String bakTimestamp = bakFile.getName().substring(prefixFilename.length());

        try {
          final Long ts = Long.valueOf(bakTimestamp);
          if (ts > latestTs) {
            latestTs = ts;
            latestBakFile = bakFile;
          }
        } catch (final NumberFormatException e) {
          LOG.warn(
              "Invalid timestamp '{}' found in backup replay file (file ignored): {}",
              bakTimestamp, bakFile.getName());
          continue;
        }
      }
    }

    return (latestBakFile != null)
        ? Optional.of(new BackupReplayFile(latestBakFile))
        : Optional.empty();
  }

  private void ensureDirectoryExists(final File backupsDir) {
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
