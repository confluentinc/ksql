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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A file that is used by the backup service to replay command_topic commands.
 */
public final class BackupReplayFile implements Closeable {
  enum Versions {
    NO_VERSION, V1
  }

  // Current version of the Backup file
  private static final String BACKUP_VERSION_HEADER = "BACKUP_VERSION";
  private static final Versions DEFAULT_BACKUP_VERSION = Versions.V1;

  // Include.ALWAYS is necessary to serialize nulls and empty values. This is required to allow
  // the CommandTopicBackupImpl to compare against all properties from a record read from
  // the command topic, which come with nulls and empty values.
  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get()
      .setSerializationInclusion(JsonInclude.Include.ALWAYS);

  private static final String KEY_VALUE_SEPARATOR = ":";

  private final File file;
  private final BufferedWriter writer;
  private final Versions version;

  public static BackupReplayFile newFile(final Path path) {
    final String versionHeader =
        String.format("%s=%s%n", BACKUP_VERSION_HEADER, DEFAULT_BACKUP_VERSION);

    try {
      // Fails if the file already exists
      Files.createFile(path, PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rw-------")
      ));

      Files.write(path, versionHeader.getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new KsqlException(
          String.format("Failed to create replay file: %s", path), e);
    }

    return new BackupReplayFile(path.toFile(), DEFAULT_BACKUP_VERSION);
  }

  public static BackupReplayFile openFile(final File file) {
    return new BackupReplayFile(file, readVersion(file));
  }

  private static Versions readVersion(final File file) {
    final Optional<String> firstLine;

    try {
      firstLine = Files.lines(file.toPath(), StandardCharsets.UTF_8).limit(1).findAny();
    } catch (final IOException e) {
      throw new KsqlException(
          String.format("Failed to read replay file: %s", file.getAbsolutePath()), e);
    }

    if (firstLine.isPresent() && isVersionLine(firstLine.get())) {
      final String backupVersion = firstLine.get().substring(BACKUP_VERSION_HEADER.length() + 1);
      return Versions.valueOf(backupVersion);
    }

    return Versions.NO_VERSION;
  }

  private static boolean isVersionLine(final String line) {
    return line.startsWith(BACKUP_VERSION_HEADER);
  }

  private BackupReplayFile(final File file, final Versions version) {
    this.version = Objects.requireNonNull(version, "version");;
    this.file = Objects.requireNonNull(file, "file");
    this.writer = createWriter(file);
  }

  private static BufferedWriter createWriter(final File file) {
    try {
      return new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file, true),
          StandardCharsets.UTF_8)
      );
    } catch (final FileNotFoundException e) {
      throw new KsqlException(
          String.format("Failed to create replay file: %s", file.getAbsolutePath()), e);
    }
  }

  public Versions getVersion() {
    return version;
  }

  public String getPath() {
    return file.getAbsolutePath();
  }

  public void write(final CommandId commandId, final Command command) throws IOException {
    writer.write(MAPPER.writeValueAsString(commandId));
    writer.write(KEY_VALUE_SEPARATOR);
    writer.write(MAPPER.writeValueAsString(command));
    writer.write("\n");
    writer.flush();
  }

  public void write(final List<Pair<CommandId, Command>> records) throws IOException {
    for (final Pair<CommandId, Command> record : records) {
      write(record.left, record.right);
    }
  }

  public List<Pair<CommandId, Command>> readRecords() throws IOException {
    final List<Pair<CommandId, Command>> commands = new ArrayList<>();
    for (final String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
      // Ignore version header
      if (isVersionLine(line)) {
        continue;
      }

      final String commandId = line.substring(0, line.indexOf(KEY_VALUE_SEPARATOR));
      final String command = line.substring(line.indexOf(KEY_VALUE_SEPARATOR) + 1);

      commands.add(new Pair<>(
          MAPPER.readValue(commandId.getBytes(StandardCharsets.UTF_8), CommandId.class),
          MAPPER.readValue(command.getBytes(StandardCharsets.UTF_8), Command.class)
      ));
    }

    return commands;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
