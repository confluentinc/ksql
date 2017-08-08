/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.commands;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.ranges.LongRange;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.console.OutputFormat;

public abstract class AbstractCliCommands implements Runnable {

  private static final String NON_INTERACTIVE_TEXT_OPTION_NAME = "--exec";
  private static final String STREAMED_QUERY_ROW_LIMIT_OPTION_NAME = "--query-row-limit";
  private static final String STREAMED_QUERY_TIMEOUT_OPTION_NAME = "--query-timeout";

  private static final String OUTPUT_FORMAT_OPTION_NAME = "--output";

  @Option(
      name = NON_INTERACTIVE_TEXT_OPTION_NAME,
      description = "Text to run non-interactively, exiting immediately after"
  )
  String nonInteractiveText;

  @Option(
      name = STREAMED_QUERY_ROW_LIMIT_OPTION_NAME,
      description = "An optional maximum number of rows to read from streamed queries"
  )

  @LongRange(
      min = 1
  )
  Long streamedQueryRowLimit;

  @Option(
      name = STREAMED_QUERY_TIMEOUT_OPTION_NAME,
      description = "An optional time limit (in milliseconds) for streamed queries"
  )
  @LongRange(
      min = 1
  )
  Long streamedQueryTimeoutMs;

  @Option(
      name = OUTPUT_FORMAT_OPTION_NAME,
      description = "The output format to use "
          + "(either 'JSON' or 'TABULAR'; can be changed during REPL as well; "
          + "defaults to TABULAR)"
  )
  String outputFormat = OutputFormat.TABULAR.name();

  @Override
  public void run() {
    try (Cli cli = getCli()) {
      if (nonInteractiveText != null) {
        cli.runNonInteractively(nonInteractiveText);
      } else {
        cli.runInteractively();
      }
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  protected abstract Cli getCli() throws Exception;

  protected OutputFormat parseOutputFormat() {
    try {
      return OutputFormat.valueOf(outputFormat.toUpperCase());
    } catch (IllegalArgumentException exception) {
      throw new ParseException(String.format("Invalid output format: '%s'", outputFormat));
    }
  }

}
