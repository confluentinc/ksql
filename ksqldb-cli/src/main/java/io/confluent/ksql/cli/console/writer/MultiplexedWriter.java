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

package io.confluent.ksql.cli.console.writer;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

/**
 * Multiplexes writes to N internal writers.
 */
public class MultiplexedWriter extends Writer {

  private final Writer[] writers;

  public MultiplexedWriter(final Writer... writers) {
    this.writers = Objects.requireNonNull(writers, "writers");
  }

  @Override
  public void write(final char[] chars, final int off, final int len) throws IOException {
    for (final Writer writer : writers) {
      writer.write(chars, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    for (final Writer writer : writers) {
      writer.flush();
    }
  }

  @Override
  public void close() throws IOException {
    for (final Writer writer : writers) {
      writer.close();
    }
  }
}
