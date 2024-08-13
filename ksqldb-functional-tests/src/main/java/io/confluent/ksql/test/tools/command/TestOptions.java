/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools.command;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import io.confluent.ksql.tools.test.command.TestOptionsParser;
import java.io.IOException;
import java.util.Optional;

@Command(name = "ksql-test-runner", description = "The KSQL testing tool")
public class TestOptions {

  @Once
  @Option(
      name = {"--input-file", "-i"},
      description = "A JSON file containing the input records.")
  private String inputFile;

  @Required
  @Once
  @Option(
      name = {"--output-file", "-o"},
      description = "A JSON file containing the expected output records.")
  private String outputFile;


  @Required
  @Once
  @Option(
      name = {"--sql-file", "-s"},
      description = "A SQL file containing KSQL statements to be tested.")
  private String statementsFile;

  @Once
  @Option(
      name = {"--extension-dir", "-e"},
      description = "A directory containing extensions.")
  private String extensionDir;

  public static TestOptions parse(final String... args) throws IOException {
    return TestOptionsParser.parse(args, TestOptions.class);
  }


  public String getInputFile() {
    return inputFile;
  }

  public String getOutputFile() {
    return outputFile;
  }

  public String getStatementsFile() {
    return statementsFile;
  }

  public Optional<String> getExtensionDir() {
    return Optional.ofNullable(extensionDir);
  }
}
