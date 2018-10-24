/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli.console;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

class JLineTerminal implements KsqlTerminal {

  private final org.jline.terminal.Terminal terminal;
  private final JLineReader lineReader;

  JLineTerminal(
      final Predicate<String> cliLinePredicate,
      final Path historyFilePath
  ) {
    this.terminal = buildTerminal();
    this.lineReader = new JLineReader(this.terminal, historyFilePath, cliLinePredicate);
  }

  @Override
  public PrintWriter writer() {
    return terminal.writer();
  }

  @Override
  public void flush() {
    terminal.flush();
  }

  @Override
  public int getWidth() {
    return terminal.getWidth();
  }

  @Override
  public void close() {
    try {
      terminal.close();
    } catch (final IOException e) {
      // Swallow
    }
  }

  @Override
  public String readLine() {
    return lineReader.readLine();
  }

  @Override
  public void clearScreen() {
    terminal.puts(InfoCmp.Capability.clear_screen);
  }

  @Override
  public void handle(
      final Terminal.Signal signal,
      final Terminal.SignalHandler signalHandler
  ) {
    terminal.handle(signal, signalHandler);
  }

  @Override
  public List<HistoryEntry> getHistory() {
    final List<HistoryEntry> history = new ArrayList<>();
    lineReader.getHistory()
        .forEach(entry -> history.add(HistoryEntry.of(entry.index() + 1, entry.line())));
    return history;
  }

  private static Terminal buildTerminal() {
    final Terminal terminal;
    try {
      terminal = TerminalBuilder.builder().system(true).build();

      // Ignore ^C when not reading a line
      terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
      return terminal;
    } catch (final IOException e) {
      throw new RuntimeException("JLineTerminal failed to start!", e);
    }
  }
}
