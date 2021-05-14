/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.cli.console;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.cli.console.writer.MultiplexedWriter;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.jline.utils.Status;

@NotThreadSafe
class JLineTerminal implements KsqlTerminal {

  private static final AttributedString DEFAULT_STATUS_MSG =
      new AttributedString("", AttributedStyle.DEFAULT);

  private final org.jline.terminal.Terminal terminal;
  private final JLineReader lineReader;
  private final Function<Terminal, Status> statusFactory;

  private Optional<Spool> spool = Optional.empty();

  JLineTerminal(
      final Predicate<String> cliLinePredicate,
      final Path historyFilePath
  ) {
    this(cliLinePredicate, historyFilePath, Status::getStatus);
  }

  JLineTerminal(
      final Predicate<String> cliLinePredicate,
      final Path historyFilePath,
      final Function<Terminal, Status> statusFactory
  ) {
    this.terminal = buildTerminal();
    this.lineReader = new JLineReader(this.terminal, historyFilePath, cliLinePredicate);
    this.statusFactory = Objects.requireNonNull(statusFactory, "statusFactory");
  }

  JLineTerminal(
      final Terminal terminal,
      final JLineReader lineReader,
      final Function<Terminal, Status> statusFactory
  ) {
    this.terminal = terminal;
    this.lineReader = lineReader;
    this.statusFactory = Objects.requireNonNull(statusFactory, "statusFactory");
  }

  @Override
  public PrintWriter writer() {
    return spool.map(Spool::getMultiplexed).orElse(terminal.writer());
  }

  @Override
  public void flush() {
    spool.map(Spool::getSpool).ifPresent(PrintWriter::flush);
    terminal.flush();
  }

  @Override
  public int getWidth() {
    return terminal.getWidth();
  }

  @Override
  public void close() {
    try {
      unsetSpool();
      terminal.close();
    } catch (final IOException e) {
      // Swallow
    }
  }

  @Override
  public String readLine() {
    final String line = lineReader.readLine();

    spool.map(Spool::getSpool)
        .ifPresent(writer -> writer.write("\nksql> " + line + "\n"));

    return line;
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
  public void setSpool(final Writer writer) {
    spool.ifPresent(ignored -> {
      throw new KsqlException("Cannot set two spools! Please issue SPOOL OFF.");
    });
    spool = Optional.of(new Spool(writer, terminal.writer()));
  }

  @Override
  public void unsetSpool() {
    spool.map(Spool::getSpool).ifPresent(PrintWriter::close);
    spool = Optional.empty();
  }

  @Override
  public List<HistoryEntry> getHistory() {
    final List<HistoryEntry> history = new ArrayList<>();
    lineReader.getHistory()
        .forEach(entry -> history.add(HistoryEntry.of(entry.index() + 1, entry.line())));
    return history;
  }

  @Override
  public StatusClosable setStatusMessage(final String message) {
    updateStatusBar(new AttributedString(message, AttributedStyle.INVERSE));
    return () -> updateStatusBar(DEFAULT_STATUS_MSG);
  }

  @Override
  public void printError(final String message) {
    writer().println(
        new AttributedString(message, AttributedStyle.DEFAULT.foreground(AttributedStyle.RED))
            .toAnsi());
  }

  @VisibleForTesting
  Terminal getTerminal() {
    return terminal;
  }

  private void updateStatusBar(final AttributedString message) {
    final Status statusBar = statusFactory.apply(terminal);
    statusBar.update(Collections.singletonList(message));
  }

  private static Terminal buildTerminal() {
    try {
      final Terminal terminal = TerminalBuilder.builder().system(true).build();

      // Ignore ^C when not reading a line
      terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
      return terminal;
    } catch (final IOException e) {
      throw new RuntimeException("JLineTerminal failed to start!", e);
    }
  }

  private static final class Spool {
    final PrintWriter spool;
    final PrintWriter multiplexed;

    private Spool(final Writer spool, final PrintWriter original) {
      Objects.requireNonNull(original, "original");

      this.spool = new PrintWriter(Objects.requireNonNull(spool, "spool"));
      this.multiplexed = new PrintWriter(new MultiplexedWriter(spool, original));
    }

    PrintWriter getSpool() {
      return spool;
    }

    PrintWriter getMultiplexed() {
      return multiplexed;
    }
  }
}
