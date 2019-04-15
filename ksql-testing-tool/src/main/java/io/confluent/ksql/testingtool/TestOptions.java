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

package io.confluent.ksql.testingtool;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import io.confluent.ksql.testingtool.util.TestOptionsParser;
import java.io.IOException;

@Command(name = "testingtool", description = "KSQL testing tool")
public class TestOptions {

  @SuppressWarnings("unused") // Accessed via reflection
  @Once
  @Required
  @Option(
      name = "--queries-file",
      description = "Path to the query file on the local machine.")
  private String queriesFile;

  @SuppressWarnings("unused") // Accessed via reflection
  @Once
  @Required
  @Option(
      name = "--test-data-file",
      description = "Path to the test data file on the local machine.")
  private String testDataFile;

  public static TestOptions parse(final String... args) throws IOException {
    return TestOptionsParser.parse(args, TestOptions.class);
  }

  public String getQueriesFile() {
    return queriesFile;
  }

  public String getTestDataFile() {
    return testDataFile;
  }
}
