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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file that is used by the backup service to replay command_topic commands.
 */
public final class BackupReplayFile implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackupReplayFile.class);

  private static final String KEY_VALUE_SEPARATOR_STR = ":";
  private static final String NEW_LINE_SEPARATOR_STR = "\n";
  private static final String TMP_SUFFIX = ".tmp";
  private static final String DIRTY_SUFFIX = ".dirty";

  private static final byte[] KEY_VALUE_SEPARATOR_BYTES =
      KEY_VALUE_SEPARATOR_STR.getBytes(StandardCharsets.UTF_8);
  private static final byte[] NEW_LINE_SEPARATOR_BYTES =
      NEW_LINE_SEPARATOR_STR.getBytes(StandardCharsets.UTF_8);

  private final File file;
  private final boolean writable;
  private final Filesystem filesystem;

  public static BackupReplayFile readOnly(final File file) throws IOException {
    return new BackupReplayFile(file, false, filesystemImpl());
  }

  public static BackupReplayFile writable(final File file) throws IOException {
    return new BackupReplayFile(file, true, filesystemImpl());
  }

  @VisibleForTesting
  BackupReplayFile(
      final File file,
      final boolean write,
      final Filesystem filesystem
  ) throws IOException {
    this.file = Objects.requireNonNull(file, "file");
    this.filesystem = Objects.requireNonNull(filesystem, "filesystem");
    this.writable = write;
    if (write) {
      initFiles();
    }
  }

  private static FileOutputStream createWriter(final File file, final Filesystem filesystem) {
    try {
      return filesystem.outputStream(file, true);
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

  private static void appendRecordToFile(
      final ConsumerRecord<byte[], byte[]> record,
      final File file,
      final Filesystem filesystem
  ) throws IOException {
    try (FileOutputStream writer = createWriter(file, filesystem)) {
      writer.write(record.key());
      writer.write(KEY_VALUE_SEPARATOR_BYTES);
      writer.write(record.value());
      writer.write(NEW_LINE_SEPARATOR_BYTES);
    }
  }

  public void write(final ConsumerRecord<byte[], byte[]> record) throws IOException {
    if (!writable) {
      throw new IOException("Write permission denied.");
    }
    final File dirty = dirty(file);
    final File tmp = tmp(file);
    // first write to the dirty copy
    appendRecordToFile(record, dirty, filesystem);
    // atomically rename the dirty copy to the "live" copy while copying the live copy to
    // the "dirty" copy via a temporary hard link
    Files.createLink(tmp.toPath(), file.toPath());
    Files.move(
        dirty.toPath(),
        file.toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
    );
    Files.move(tmp.toPath(), dirty.toPath());
    // keep the dirty copy in sync with the live copy, which now has the write
    appendRecordToFile(record, dirty, filesystem);
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
  public void close() {
  }

  private void initFiles() throws IOException {
    final Path dirtyFile = dirty(file).toPath();
    Files.deleteIfExists(dirtyFile);
    Files.deleteIfExists(tmp(file).toPath());
    if (file.createNewFile()) {
      LOGGER.info("created new backup replay file {}", file.getAbsolutePath());
    }
    Files.copy(file.toPath(), dirty(file).toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  private File dirty(final File file) {
    return new File(file.getPath() + DIRTY_SUFFIX);
  }

  private File tmp(final File file) {
    return new File(file.getPath() + TMP_SUFFIX);
  }

  private static Filesystem filesystemImpl() {
    return FileOutputStream::new;
  }

  @VisibleForTesting
  interface Filesystem {
    FileOutputStream outputStream(File file, boolean append)
        throws FileNotFoundException;
  }
}
