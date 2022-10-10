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

package io.confluent.ksql.tools.test;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.tools.test.command.SqlTestOptions;
import io.confluent.ksql.tools.test.parser.SqlTestLoader;
import io.confluent.ksql.tools.test.parser.SqlTestLoader.SqlTest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SqlTestingTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlTestingTool.class);

  private SqlTestingTool() {
  }

  public static void main(final String[] args) {
    try {
      final int code = run(args);
      System.exit(code);
    } catch (final Exception e) {
      System.exit(1);
    }
  }

  @VisibleForTesting
  static int run(final String[] args) throws IOException {
    final SqlTestOptions testOptions = SqlTestOptions.parse(args);
    if (testOptions == null) {
      return 1;
    }

    final Path tempFolder = Paths.get(testOptions.getTempFolder());
    final SqlTestLoader loader = new SqlTestLoader(Paths.get(testOptions.getTestDirectory()));
    loader.load().forEach(
        test -> executeTest(test, SqlTestExecutor.create(tempFolder), tempFolder.toFile()));

    return 0;
  }

  private static void executeTest(
      final SqlTest test,
      final SqlTestExecutor executor,
      final File tempFolder
  ) {
    try {
      executor.executeTest(test);
      LOGGER.info("\t >>> Test passed!");
    } catch (final Throwable e) {
      LOGGER.error("\t>>>>> Test failed: " + e.getMessage());
    } finally {
      cleanUp(executor, tempFolder);
    }
  }

  private static void cleanUp(final SqlTestExecutor executor, final File tempFolder) {
    executor.close();
    try {
      FileUtils.cleanDirectory(tempFolder);
    } catch (final Exception e) {
      LOGGER.warn("Failed to clean up temp folder: " + e.getMessage());
    }
  }
}
