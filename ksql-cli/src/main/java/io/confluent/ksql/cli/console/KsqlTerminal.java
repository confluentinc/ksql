/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console;

import java.io.Closeable;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import org.jline.terminal.Terminal;

public interface KsqlTerminal {

  int getWidth();

  PrintWriter writer();

  String readLine();

  void flush();

  void clearScreen();

  List<HistoryEntry> getHistory();

  void handle(Terminal.Signal signal, Terminal.SignalHandler signalHandler);

  @FunctionalInterface
  interface StatusClosable extends Closeable {

    void close();
  }

  /**
   * Set a status message in the terminal.
   *
   * <p>The message is removed once the returned {@link StatusClosable} is closed.
   *
   * @param message the message to display
   * @return the closable that will remove the status message.
   */
  StatusClosable setStatusMessage(String message);

  void close();

  class HistoryEntry {
    final long index;
    final String line;

    private HistoryEntry(final long index, final String line) {
      this.index = index;
      this.line = Objects.requireNonNull(line, "line");
      if (index < 1) {
        throw new IllegalArgumentException("index < 1. index=" + index);
      }
    }

    static HistoryEntry of(final long index, final String line) {
      return new HistoryEntry(index, line);
    }
  }
}
