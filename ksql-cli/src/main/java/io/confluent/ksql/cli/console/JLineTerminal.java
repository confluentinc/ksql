/**
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

import io.confluent.ksql.rest.client.KsqlRestClient;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.jline.utils.Status;

public class JLineTerminal extends Console {

  private final org.jline.terminal.Terminal terminal;

  public JLineTerminal(final OutputFormat outputFormat, final KsqlRestClient restClient) {
    super(outputFormat, restClient);

    try {
      terminal = TerminalBuilder.builder().system(true).build();
    } catch (final IOException e) {
      throw new RuntimeException("JLineTerminal failed to start!", e);
    }
    // Ignore ^C when not reading a line
    terminal.handle(
        org.jline.terminal.Terminal.Signal.INT,
        org.jline.terminal.Terminal.SignalHandler.SIG_IGN
    );
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
  public void close() throws IOException {
    terminal.close();
  }

  /* jline specific */

  @Override
  protected JLineReader buildLineReader() {
    final Path historyFilePath = Paths.get(System.getProperty(
        "history-file",
        System.getProperty("user.home")
        + "/.ksql-history"
    )).toAbsolutePath();
    return new JLineReader(this.terminal, historyFilePath);
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
  public void printHowToInterruptMsg() {
    final Status statusBar = Status.getStatus(terminal);
    final AttributedStringBuilder sb = new AttributedStringBuilder();
    sb.style(AttributedStyle.INVERSE);
    sb.append("Press CTRL-C to interrupt");
    statusBar.update(Arrays.asList(sb.toAttributedString()));
  }

  @Override
  public void clearStatusMsg() {
    final Status statusBar = Status.getStatus(terminal);
    statusBar.update(Arrays.asList(new AttributedString("", AttributedStyle.DEFAULT)));
    statusBar.reset();
    statusBar.redraw();
  }

}
