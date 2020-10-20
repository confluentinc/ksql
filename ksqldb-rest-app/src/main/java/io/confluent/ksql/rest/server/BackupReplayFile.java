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

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A file that is used by the backup service to replay command_topic commands.
 */
public final class BackupReplayFile implements Closeable {
  private static final String KEY_VALUE_SEPARATOR_STR = ":";
  private static final String NEW_LINE_SEPARATOR_STR = "\n";

  private static final byte[] KEY_VALUE_SEPARATOR_BYTES =
      KEY_VALUE_SEPARATOR_STR.getBytes(StandardCharsets.UTF_8);
  private static final byte[] NEW_LINE_SEPARATOR_BYTES =
      NEW_LINE_SEPARATOR_STR.getBytes(StandardCharsets.UTF_8);

  private final File file;
  private final FileOutputStream writer;

  public static BackupReplayFile readOnly(final File file) {
    return new BackupReplayFile(file, false);
  }

  public static BackupReplayFile writable(final File file) {
    return new BackupReplayFile(file, true);
  }

  private BackupReplayFile(final File file, final boolean write) {
    this.file = Objects.requireNonNull(file, "file");

    if (write) {
      this.writer = createWriter(file);
    } else {
      this.writer = null;
    }
  }

  private static FileOutputStream createWriter(final File file) {
    try {
      return new FileOutputStream(file, true);
    } catch (final FileNotFoundException e) {
      throw new KsqlException(
          String.format("Failed to create/open replay file: %s", file.getAbsolutePath()), e);
    }
  }

  public File getFile() {
    return file;
  }

  public String getPath() {
    return file.getAbsolutePath();
  }

  public void write(final ConsumerRecord<byte[], byte[]> record) throws IOException {
    if (writer == null) {
      throw new IOException("Write permission denied.");
    }

    writer.write(record.key());
    writer.write(KEY_VALUE_SEPARATOR_BYTES);
    writer.write(record.value());
    writer.write(NEW_LINE_SEPARATOR_BYTES);
    writer.flush();
  }

  public List<Pair<byte[], byte[]>> readRecords() throws IOException {
    final List<Pair<byte[], byte[]>> commands = new ArrayList<>();
    for (final String line : Files.readAllLines(getFile().toPath(), StandardCharsets.UTF_8)) {
      final String commandId = line.substring(0, line.indexOf(KEY_VALUE_SEPARATOR_STR));
      final String command = line.substring(line.indexOf(KEY_VALUE_SEPARATOR_STR) + 1);

      commands.add(new Pair<>(
          commandId.getBytes(StandardCharsets.UTF_8),
          command.getBytes(StandardCharsets.UTF_8)
      ));
    }

    return commands;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }
}
