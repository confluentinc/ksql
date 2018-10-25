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

package io.confluent.ksql;

import io.confluent.ksql.cli.console.KsqlTerminal;
import java.io.PrintWriter;
import java.io.StringWriter;
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
}
