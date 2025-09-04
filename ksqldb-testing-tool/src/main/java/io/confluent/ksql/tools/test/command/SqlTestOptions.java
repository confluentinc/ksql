/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.tools.test.command;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.IOException;

@Command(name = "sql-test-runner", description = "The KSQL SQL testing tool")
public class SqlTestOptions {
  @Required
  @Once
  @Option(
      name = {"--test-directory", "-td"},
      description = "A directory containing SQL files to test.")
  private String testDirectory;

  @Required
  @Once
  @Option(
      name = {"--temp-folder", "-tf"},
      description = "A folder to store temporary files")
  private String tempFolder;

  public static SqlTestOptions parse(final String... args) throws IOException {
    return TestOptionsParser.parse(args, SqlTestOptions.class);
  }

  public String getTestDirectory() {
    return testDirectory;
  }

  public String getTempFolder() {
    return tempFolder;
  }
}
