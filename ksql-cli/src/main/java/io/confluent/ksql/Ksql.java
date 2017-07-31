/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.ksql.cli.commands.Local;
import io.confluent.ksql.cli.commands.Remote;
import io.confluent.ksql.cli.commands.Standalone;

import java.io.IOException;

public class Ksql {

  public static void main(String[] args) throws IOException {
    Runnable runnable = null;
    com.github.rvesse.airline.Cli<Runnable> cli =
        com.github.rvesse.airline.Cli.<Runnable>builder("Cli")
            .withDescription("Kafka Query Language")
            .withDefaultCommand(Help.class)
            .withCommand(Local.class)
            .withCommand(Remote.class)
            .withCommand(Standalone.class)
            .build();

    try {
      runnable = cli.parse(args);
      runnable.run();
    } catch (ParseException exception) {
      if (exception.getMessage() != null) {
        System.err.println(exception.getMessage());
      } else {
        System.err.println("Options parsing failed for an unknown reason");
      }
      System.err.println("See the help command for usage information");
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    if ((runnable != null) && !(runnable instanceof Standalone)) {
      System.exit(0);
    }

  }
}
