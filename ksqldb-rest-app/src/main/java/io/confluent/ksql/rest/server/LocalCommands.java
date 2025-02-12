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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents commands that are issued to this node only (therefore not written to the command
 * topic), and might require additional cleanup later.  An example is a transient query.
 * These commands are stored locally on disk so that we can do any cleanup required, next time
 * the server restarts. This is only appropriate for best effort tasks since the data isn't durable
 * (since a node might leave and never rejoin the cluster).
 */
public class LocalCommands implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(LocalCommands.class);

  static final String LOCAL_COMMANDS_FILE_SUFFIX = ".cmds";
  static final String LOCAL_COMMANDS_PROCESSED_SUFFIX = ".processed";
  private static final Random RANDOM = new Random();

  private final File directory;
  private final KsqlEngine ksqlEngine;
  private final LocalCommandsFile currentLocalCommands;

  LocalCommands(
      final File directory,
      final KsqlEngine ksqlEngine,
      final LocalCommandsFile currentLocalCommands
  ) {
    this.directory = directory;
    this.ksqlEngine = ksqlEngine;
    this.currentLocalCommands = currentLocalCommands;
  }

  public File getCurrentLocalCommandsFile() {
    return currentLocalCommands.getFile();
  }

  public void processLocalCommandFiles(
      final ServiceContext serviceContext
  ) {
    final FilenameFilter filter = (dir, fileName) -> fileName.endsWith(LOCAL_COMMANDS_FILE_SUFFIX);
    final File[] files = directory.listFiles(filter);
    if (files == null) {
      throw new KsqlServerException("Bad local commands directory " + directory.getAbsolutePath()
          + ". Please check your configuration for "
          + KsqlRestConfig.KSQL_LOCAL_COMMANDS_LOCATION_CONFIG);
    }
    for (final File file : files) {
      if (file.equals(currentLocalCommands.getFile())) {
        continue;
      }
      try (LocalCommandsFile localCommandsFile = LocalCommandsFile.createReadonly(file)) {
        final List<LocalCommand> localCommands = localCommandsFile.readRecords();
        cleanUpTransientQueryState(localCommands, serviceContext);

        markFileAsProcessed(file);
      } catch (Exception e) {
        LOG.error("Error processing local commands " + file.getAbsolutePath()
                + ". There may be orphaned transient topics or abandoned state stores.", e);
      }
    }
  }

  public void write(final TransientQueryMetadata queryMetadata) {
    try {
      currentLocalCommands.write(
          new TransientQueryLocalCommand(queryMetadata.getQueryApplicationId()));
    } catch (IOException e) {
      // Just log an error since not catching it would likely cause more cleanup work than this
      // aims to fix.
      LOG.error("Failed to write local command for transient query:"
          + queryMetadata.getQueryApplicationId(), e);
    }
  }

  @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE")
  public static LocalCommands open(
      final KsqlEngine ksqlEngine,
      final File directory
  ) {
    if (!directory.exists()) {
      if (!directory.mkdirs()) {
        throw new KsqlServerException("Couldn't create the local commands directory: "
            + directory.getPath()
            + "\n Make sure the directory exists and is readable/writable for KSQL server "
            + "\n or its parent directory is readable/writable by KSQL server"
            + "\n or change it to a readable/writable directory by setting '"
            + KsqlRestConfig.KSQL_LOCAL_COMMANDS_LOCATION_CONFIG
            + "' config in the properties file."
        );
      }

      try {
        Files.setPosixFilePermissions(directory.toPath(),
            PosixFilePermissions.fromString("rwx------"));
      } catch (final IOException e) {
        throw new KsqlServerException(String.format(
            "Couldn't set POSIX permissions on the backups directory: %s. Error = %s",
            directory.getPath(), e.getMessage()));
      }
    }

    if (!directory.isDirectory()) {
      throw new KsqlServerException(directory.getPath()
          + " is not a directory."
          + "\n Make sure the directory exists and is readable/writable for KSQL server "
          + "\n or its parent directory is readable/writable by KSQL server"
          + "\n or change it to a readable/writable directory by setting '"
          + KsqlRestConfig.KSQL_LOCAL_COMMANDS_LOCATION_CONFIG
          + "' config in the properties file."
      );
    }

    if (!directory.canWrite() || !directory.canRead() || !directory.canExecute()) {
      throw new KsqlServerException("The local commands directory is not readable/writable "
          + "for KSQL server: "
          + directory.getPath()
          + "\n Make sure the directory exists and is readable/writable for KSQL server "
          + "\n or change it to a readable/writable directory by setting '"
          + KsqlRestConfig.KSQL_LOCAL_COMMANDS_LOCATION_CONFIG
          + "' config in the properties file."
      );
    }
    final File file = new File(directory, String.format("local_commands_%d_%s%s",
        System.currentTimeMillis(), Integer.toHexString(RANDOM.nextInt()),
        LOCAL_COMMANDS_FILE_SUFFIX));
    return new LocalCommands(directory, ksqlEngine, LocalCommandsFile.createWriteable(file));
  }

  private void markFileAsProcessed(final File file) {
    final File updatedName = new File(file.getParentFile(),
        file.getName() + LOCAL_COMMANDS_PROCESSED_SUFFIX);
    if (!file.renameTo(updatedName)) {
      throw new KsqlException("Couldn't rename file " + file.getAbsolutePath());
    }
  }

  private void cleanUpTransientQueryState(
      final List<LocalCommand> localCommands,
      final ServiceContext serviceContext) {
    final Set<String> queryApplicationIds = localCommands.stream()
        .filter(c -> c.getType().equals(TransientQueryLocalCommand.TYPE))
        .map(TransientQueryLocalCommand.class::cast)
        .map(TransientQueryLocalCommand::getQueryApplicationId)
        .collect(Collectors.toSet());
    if (queryApplicationIds.size() > 0) {
      ksqlEngine.cleanupOrphanedInternalTopics(serviceContext, queryApplicationIds);
      ksqlEngine.populateTransientQueryCleanupServiceWithOldCommands(queryApplicationIds);
    }
  }

  @Override
  public void close() throws IOException {
    currentLocalCommands.close();
  }
}
