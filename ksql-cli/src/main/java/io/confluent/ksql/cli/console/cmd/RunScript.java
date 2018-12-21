/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import com.google.common.io.Files;
import io.confluent.ksql.cli.KsqlRequestExecutor;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Collectors;

class RunScript implements CliSpecificCommand {

  private final Console console;
  private final KsqlRequestExecutor requestExecutor;

  RunScript(
      final Console console,
      final KsqlRequestExecutor requestExecutor
  ) {
    this.console = Objects.requireNonNull(console, "console");
    this.requestExecutor = Objects.requireNonNull(requestExecutor, "requestExecutor");
  }

  @Override
  public String getName() {
    return "run";
  }

  @Override
  public void printHelp() {
    console.writer().println("run <path_to_sql_file>:");
    console.writer().println("\tLoad and run the statements in the supplied file.");
    console.writer().println("\tNote: the file must be UTF-8 encoded.");
  }

  @Override
  public void execute(final String command) {
    final String filePath = StringUtil.cleanQuotes(command.trim());
    final String content = loadScript(filePath);
    requestExecutor.makeKsqlRequest(content);
  }

  private static String loadScript(final String filePath) {
    try {
      return Files.readLines(new File(filePath), StandardCharsets.UTF_8).stream()
          .collect(Collectors.joining(System.lineSeparator()));
    } catch (IOException e) {
      throw new KsqlException("Failed to read file: " + filePath, e);
    }
  }
}
