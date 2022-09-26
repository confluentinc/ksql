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

package io.confluent.ksql.test.tools;

import io.confluent.ksql.test.parser.SqlTestLoader;
import io.confluent.ksql.test.parser.SqlTestLoader.SqlTest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

public final class SqlTestingTool {

  private SqlTestingTool() {
  }

  public static void main(final String[] args) throws IOException {
    SqlTestOptions testOptions = null;
    try {
      testOptions = SqlTestOptions.parse(args);
    } catch (final Exception e) {
      System.err.println("Invalid arguments: " + e.getMessage());
      System.exit(1);
      return;
    }

    final Path tempFolder = Paths.get(testOptions.getTempFolder());
    final SqlTestLoader loader = new SqlTestLoader(Paths.get(testOptions.getTestDirectory()));
    loader.load().forEach(
        test -> executeTest(test, SqlTestExecutor.create(tempFolder), tempFolder.toFile()));

    System.exit(0);
    return;
  }

  private static void executeTest(
      final SqlTest test,
      final SqlTestExecutor executor,
      final File tempFolder
  ) {
    try {
      executor.executeTest(test);
      System.out.println("\t >>> Test passed!");
    } catch (final Throwable e) {
      System.err.println("\t>>>>> Test failed: " + e.getMessage());
    } finally {
      cleanUp(executor, tempFolder);
    }
  }

  private static void cleanUp(final SqlTestExecutor executor, final File tempFolder) {
    executor.close();
    try {
      FileUtils.cleanDirectory(tempFolder);
    } catch (final Exception e) {
      System.err.println("Failed to clean up temp folder: " + e.getMessage());
    }
  }
}
