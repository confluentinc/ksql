/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import io.confluent.ksql.cli.console.LineReader;
import org.jline.reader.History;

import java.io.IOException;
import java.util.ArrayList;

public class TestLineReader implements LineReader {

  @Override
  public String readLine() throws IOException {
    return null;
  }

  @Override
  public Iterable<? extends History.Entry> getHistory() {
    return new ArrayList<>();
  }
}
