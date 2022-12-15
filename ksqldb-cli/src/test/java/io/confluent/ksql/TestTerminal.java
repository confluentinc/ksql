/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.cli.console.KsqlTerminal;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.jline.terminal.Terminal;

public class TestTerminal implements KsqlTerminal {

  private final PrintWriter printWriter;
  private final StringWriter writer;
  private final Supplier<String> lineSupplier;

  public TestTerminal(final Supplier<String> lineSupplier) {
    this.lineSupplier = lineSupplier;
    this.writer = new StringWriter();
    this.printWriter = new PrintWriter(writer);
  }

  public String getOutputString() {
    return writer.toString();
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "should be mutable")
  public PrintWriter writer() {
    return printWriter;
  }

  @Override
  public int getWidth() {
    return 100;
  }

  @Override
  public void flush() {
    printWriter.flush();
  }

  @Override
  public void close() {
    printWriter.close();
  }

  @Override
  public String readLine() {
    return lineSupplier.get();
  }

  @Override
  public List<HistoryEntry> getHistory() {
    return new ArrayList<>();
  }

  @Override
  public void clearScreen() {
    // Ignore
  }

  @Override
  public void handle(final Terminal.Signal signal, final Terminal.SignalHandler signalHandler) {
    // Ignore
  }

  @Override
  public void setSpool(final Writer writer) {
    // Ignore
  }

  @Override
  public void unsetSpool() {
    // Ignore
  }

  @Override
  public StatusClosable setStatusMessage(final String message) {
    return () -> {};
  }

  @Override
  public void printError(final String message) {
    writer().println(message);
  }
}
