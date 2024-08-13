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
import io.confluent.ksql.util.KsqlException;
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

/**
 * Represents a single file of commands issued to this node.
 */
public final class LocalCommandsFile  implements Closeable {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final byte[] NEW_LINE_SEPARATOR_BYTES =
      "\n".getBytes(StandardCharsets.UTF_8);


  private final File file;
  private final FileOutputStream writer;

  private LocalCommandsFile(final File file, final boolean write) {
    this.file = Objects.requireNonNull(file, "file");

    if (write) {
      this.writer = createWriter(file);
    } else {
      this.writer = null;
    }
  }

  public static LocalCommandsFile createReadonly(final File file) {
    return new LocalCommandsFile(file, false);
  }

  public static LocalCommandsFile createWriteable(final File file) {
    return new LocalCommandsFile(file, true);
  }

  public synchronized void write(final LocalCommand localCommand) throws IOException {
    if (writer == null) {
      throw new IOException("Write permission denied.");
    }

    final byte[] bytes = MAPPER.writeValueAsBytes(localCommand);
    writer.write(bytes);
    writer.write(NEW_LINE_SEPARATOR_BYTES);
    writer.flush();
  }

  public List<LocalCommand> readRecords() throws IOException {
    final List<LocalCommand> localCommands = new ArrayList<>();
    for (final String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
      final LocalCommand localCommand = MAPPER.readValue(line, LocalCommand.class);
      localCommands.add(localCommand);
    }
    return localCommands;
  }

  @Override
  public synchronized void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  public File getFile() {
    return file;
  }

  private static FileOutputStream createWriter(final File file) {
    try {
      return new FileOutputStream(file, true);
    } catch (final FileNotFoundException e) {
      throw new KsqlException(
          String.format("Failed to create/open replay file: %s", file.getAbsolutePath()), e);
    }
  }
}
