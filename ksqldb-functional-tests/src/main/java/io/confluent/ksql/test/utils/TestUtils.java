/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.utils;

import io.confluent.common.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Version of {@link io.confluent.common.utils.TestUtils} that doesn't create thousands of threads
 * to delete temporary directories when the program finishes.
 */
public enum TestUtils {

  INSTANCE;

  private final Path root;

  TestUtils() {
    this.root = createRoot();
  }

  /**
   * Create a temporary directory. The directory and any contents will be deleted when the test
   * process terminates.
   */
  public static Path tempDirectory() {
    return INSTANCE.createTempDirectory();
  }

  private Path createTempDirectory() {
    try {
      return Files.createTempDirectory(root, null);
    } catch (final IOException ex) {
      throw new RuntimeException("Failed to create a temp dir", ex);
    }
  }

  private static Path createRoot() {
    try {
      final Path path = Files.createTempDirectory("confluent-ksqldb");

      // Single shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            Utils.delete(path.toFile());
          } catch (IOException e) {
            System.err.println("Error deleting " + path.toAbsolutePath());
          }
        }
      });

      return path;
    } catch (final IOException ex) {
      throw new RuntimeException("Failed to create a temp dir", ex);
    }
  }
}
