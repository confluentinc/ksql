/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.console;

import org.jline.reader.History;

import java.io.IOException;

public interface LineReader {
  Iterable<? extends History.Entry> getHistory();

  String readLine() throws IOException;
}
