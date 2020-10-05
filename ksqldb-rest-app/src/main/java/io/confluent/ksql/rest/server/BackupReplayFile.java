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
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A file that is used by the backup service to replay command_topic commands.
 */
public final class BackupReplayFile extends BackupInputFile implements Closeable {
  private final FileOutputStream writer;

  public BackupReplayFile(final File file) {
    super(file);
    this.writer = createWriter(file);
  }

  private static FileOutputStream createWriter(final File file) {
    try {
      return new FileOutputStream(file, true);
    } catch (final FileNotFoundException e) {
      throw new KsqlException(
          String.format("Failed to create/open replay file: %s", file.getAbsolutePath()), e);
    }
  }

  public String getPath() {
    return getFile().getAbsolutePath();
  }

  public void write(final ConsumerRecord<byte[], byte[]> record) throws IOException {
    writer.write(record.key());
    writer.write(KEY_VALUE_SEPARATOR_BYTES);
    writer.write(record.value());
    writer.write(NEW_LINE_SEPARATOR_BYTES);
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
