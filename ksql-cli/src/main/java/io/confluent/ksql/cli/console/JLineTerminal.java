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

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.IOException;
import java.io.PrintWriter;

import io.confluent.ksql.rest.client.KsqlRestClient;

public class JLineTerminal extends Console {

  private final org.jline.terminal.Terminal terminal;

  public JLineTerminal(OutputFormat outputFormat, KsqlRestClient restClient) {
    super(outputFormat, restClient);

    try {
      terminal = TerminalBuilder.builder().system(true).build();
    } catch (IOException e) {
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
    return new JLineReader(this.terminal);
  }

  @Override
  public void puts(InfoCmp.Capability capability) {
    terminal.puts(capability);
  }

  @Override
  public Terminal.SignalHandler handle(
      Terminal.Signal signal,
      Terminal.SignalHandler signalHandler
  ) {
    return terminal.handle(signal, signalHandler);
  }

}
