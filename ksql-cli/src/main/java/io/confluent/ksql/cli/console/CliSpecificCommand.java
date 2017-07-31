/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.console;

import java.io.IOException;

public interface CliSpecificCommand {
  String getName();

  void printHelp();

  void execute(String commandStrippedLine) throws IOException;
}
