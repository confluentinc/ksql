/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import com.google.common.collect.ImmutableSet;

import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public final class Completion implements Completer {

  private static final Set<String> COMMANDS = ImmutableSet.of(
      "SELECT",
      "SHOW CATALOGS",
      "SHOW COLUMNS",
      "SHOW FUNCTIONS",
      "SHOW PARTITIONS",
      "SHOW SCHEMAS",
      "SHOW SESSION",
      "SHOW TABLES",
      "CREATE TABLE",
      "DROP TABLE",
      "EXPLAIN",
      "DESCRIBE",
      "USE",
      "HELP",
      "QUIT");

  private Completion() {
  }

  public static Completer commandCompleter() {
    return new StringsCompleter(COMMANDS);
  }

  // TODO: create a case-insensitive completer
  public static Completer lowerCaseCommandCompleter() {
    return new StringsCompleter(COMMANDS.stream()
                                    .map(s -> s.toLowerCase(Locale.ENGLISH))
                                    .collect(toSet()));
  }

  @Override
  public int complete(String s, int i, List<CharSequence> list) {
    return 0;
  }
}