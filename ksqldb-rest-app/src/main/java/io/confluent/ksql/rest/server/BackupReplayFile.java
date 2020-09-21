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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A file that is used by the backup service to replay command_topic commands.
 */
public class BackupReplayFile implements Closeable {
  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();
  private static final String KEY_VALUE_SEPARATOR = ":";

  private final File file;
  private final BufferedWriter writer;

  public BackupReplayFile(final File file) {
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
