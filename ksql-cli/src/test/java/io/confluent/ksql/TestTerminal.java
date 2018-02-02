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

package io.confluent.ksql;

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.OutputFormat;

import io.confluent.ksql.rest.client.KsqlRestClient;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class TestTerminal extends Console {

  private final PrintWriter printWriter;
  private final StringWriter writer;
  private TestResult output;

  public TestTerminal(OutputFormat outputFormat, KsqlRestClient restClient) {
    super(outputFormat, restClient);

    this.writer = new StringWriter();
    this.printWriter = new PrintWriter(writer);

    resetTestResult(true);
  }

  public void resetTestResult(boolean requireOrder) {
    output = TestResult.init(requireOrder);
  }

  public synchronized TestResult getTestResult() {
    return output.copy();
  }

  public String getOutputString() {
    return writer.toString();
  }

  @Override
  public synchronized void addResult(GenericRow row) {
    output.addRow(row);
  }

  @Override
  public void addResult(List<String> columnHeaders, List<List<String>> rows) {
    output.addRows(rows);
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
  protected TestLineReader buildLineReader() {
    return new TestLineReader();
  }

  @Override
  protected void puts(InfoCmp.Capability capability) {
    // Ignore
  }

  @Override
  public Terminal.SignalHandler handle(Terminal.Signal signal, Terminal.SignalHandler signalHandler) {
    // Ignore
    return null;
  }
}
