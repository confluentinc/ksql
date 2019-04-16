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

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.client.KsqlRestClient;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.jline.terminal.Terminal;

public class TestTerminal extends Console {

  private final PrintWriter printWriter;
  private final StringWriter writer;
  private TestResult.Builder output;

  public TestTerminal(final OutputFormat outputFormat, final KsqlRestClient restClient) {
    super(outputFormat, restClient);

    this.writer = new StringWriter();
    this.printWriter = new PrintWriter(writer);

    resetTestResult(true);
  }

  public void resetTestResult(final boolean requireOrder) {
    output = new TestResult.Builder();
  }

  public synchronized TestResult getTestResult() {
    return output.build();
  }

  public String getOutputString() {
    return writer.toString();
  }

  @Override
  public synchronized void addResult(final GenericRow row) {
    output.addRow(row);
  }

  @Override
  public void addResult(final List<String> columnHeaders, final List<List<String>> rows) {
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
  public void clearScreen() {
    // Ignore
  }

  @Override
  public void handle(final Terminal.Signal signal, final Terminal.SignalHandler signalHandler) {
    // Ignore
  }
}
