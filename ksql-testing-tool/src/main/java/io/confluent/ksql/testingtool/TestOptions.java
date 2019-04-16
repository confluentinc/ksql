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

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import io.confluent.ksql.testingtool.util.TestOptionsParser;
import java.io.IOException;

@Command(name = "testingtool", description = "KSQL testing tool")
public class TestOptions {

  @SuppressWarnings("unused") // Accessed via reflection
  @Once
  @Required
  @Arguments(
      title = "test-file",
      description = "A JSON file containing the test configurations"
          + " including statements and test data.")
  private String testFile;


  public static TestOptions parse(final String... args) throws IOException {
    return TestOptionsParser.parse(args, TestOptions.class);
  }

  public String getTestFile() {
    return testFile;
  }
}
