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

package io.confluent.ksql.cli.console.cmd;

import io.confluent.ksql.cli.KsqlRequestExecutor;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class RunScript implements CliSpecificCommand {

  private static final String HELP = "run script <path_to_sql_file>:" + System.lineSeparator()
      + "\tLoad and run the statements in the supplied file." + System.lineSeparator()
      + "\tNote: the file must be UTF-8 encoded.";

  private final KsqlRequestExecutor requestExecutor;

  private RunScript(final KsqlRequestExecutor requestExecutor) {
    this.requestExecutor = Objects.requireNonNull(requestExecutor, "requestExecutor");
  }

  public static RunScript create(final KsqlRequestExecutor requestExecutor) {
    return new RunScript(requestExecutor);
  }

  @Override
  public String getName() {
    return "run script";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 1, 1, HELP);

    final String filePath = args.get(0);
    final String content = loadScript(filePath);
    requestExecutor.makeKsqlRequest(content);
  }

  private static String loadScript(final String filePath) {
    try {
      return Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8).stream()
          .collect(Collectors.joining(System.lineSeparator()));
    } catch (final IOException e) {
      throw new KsqlException("Failed to read file: " + filePath, e);
    }
  }
}
