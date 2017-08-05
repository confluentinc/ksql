/**
 * Copyright 2017 Confluent Inc.
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

public class TestTerminal extends Console {

  private final PrintWriter printWriter;
  private final StringWriter writer;

  public TestTerminal(OutputFormat outputFormat, KsqlRestClient restClient) {
    super(outputFormat, restClient);

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
  public void close() throws IOException {
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
