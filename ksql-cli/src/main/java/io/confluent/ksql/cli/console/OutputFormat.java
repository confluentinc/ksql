/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.console;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum OutputFormat {
  JSON,
  TABULAR;

  public static final String VALID_FORMATS = String.format(
      "'%s'",
      String.join(
          "', '",
          Arrays.stream(OutputFormat.values())
              .map(Object::toString)
              .collect(Collectors.toList())
      )
  );

  public static OutputFormat get(String format) throws IllegalArgumentException {
    return OutputFormat.valueOf(format);
  }
}
