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

package io.confluent.ksql.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.ranges.LongRange;

import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.util.OptionsParser;
import io.confluent.ksql.util.Pair;
import java.io.IOException;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.kafka.common.config.ConfigException;

@Command(name = "ksql", description = "KSQL CLI")
public class Options {

  private static final String STREAMED_QUERY_ROW_LIMIT_OPTION_NAME = "--query-row-limit";
  private static final String STREAMED_QUERY_TIMEOUT_OPTION_NAME = "--query-timeout";
  private static final String USERNAME_OPTION = "--user";
  private static final String USERNAME_SHORT_OPTION = "-u";
  private static final String PASSWORD_OPTION = "--password";
  private static final String PASSWORD_SHORT_OPTION = "-p";
  private static final String OUTPUT_FORMAT_OPTION_NAME = "--output";

  // Only here so that the help message generated by Help.help() is accurate
  @Inject
  public HelpOption help;

  @SuppressWarnings("unused") // Accessed via reflection
  @Once
  @Required
  @Arguments(
      title = "server",
      description = "The address of the Ksql server to connect to (ex: http://confluent.io:9098)")
  private String server;

  private static final String CONFIGURATION_FILE_OPTION_NAME = "--config-file";

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = CONFIGURATION_FILE_OPTION_NAME,
      description = "A file specifying configs for Ksql and its underlying Kafka Streams "
          + "instance(s). Refer to KSQL documentation for a list of available configs.")
  private String configFile;


  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = {USERNAME_OPTION, USERNAME_SHORT_OPTION},
      description =
          "If your KSQL server is configured for authentication, then provide your user name here. "
              + "The password must be specified separately with the "
              + PASSWORD_SHORT_OPTION
              + "/"
              + PASSWORD_OPTION
              + " flag",
      hidden = true)
  private String userName;

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = {PASSWORD_OPTION, PASSWORD_SHORT_OPTION},
      description =
          "If your KSQL server is configured for authentication, then provide your password here. "
              + "The username must be specified separately with the "
              + USERNAME_SHORT_OPTION
              + "/"
              + USERNAME_OPTION
              + " flag",
      hidden = true)
  private String password;

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = STREAMED_QUERY_ROW_LIMIT_OPTION_NAME,
      description = "An optional maximum number of rows to read from streamed queries")
  @LongRange(
      min = 1)
  private Long streamedQueryRowLimit;

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = STREAMED_QUERY_TIMEOUT_OPTION_NAME,
      description = "An optional time limit (in milliseconds) for streamed queries")
  @LongRange(
      min = 1)
  private Long streamedQueryTimeoutMs;

  @SuppressWarnings("FieldMayBeFinal") // Accessed via reflection
  @Option(
      name = OUTPUT_FORMAT_OPTION_NAME,
      description = "The output format to use "
          + "(either 'JSON' or 'TABULAR'; can be changed during REPL as well; "
          + "defaults to TABULAR)")
  private String outputFormat = OutputFormat.TABULAR.name();

  public static Options parse(final String...args) throws IOException {
    return OptionsParser.parse(args, Options.class);
  }

  public String getServer() {
    return server;
  }

  public Optional<String> getConfigFile() {
    return Optional.ofNullable(configFile);
  }

  public Long getStreamedQueryRowLimit() {
    return streamedQueryRowLimit;
  }

  public Long getStreamedQueryTimeoutMs() {
    return streamedQueryTimeoutMs;
  }

  public OutputFormat getOutputFormat() {
    return OutputFormat.valueOf(outputFormat);
  }

  public Optional<Pair<String, String>> getUserNameAndPassword() {
    if ((userName == null && password != null) || (password == null && userName != null)) {
      throw new ConfigException(
          "You must specify both a username and a password. If you don't want to use an "
              + "authenticated session, don't specify either of the "
              + USERNAME_OPTION
              + " or the "
              + PASSWORD_OPTION
              + " flags on the command line");
    }

    if (userName == null) {
      return Optional.empty();
    }

    return Optional.of(new Pair<>(userName, password));
  }
}
