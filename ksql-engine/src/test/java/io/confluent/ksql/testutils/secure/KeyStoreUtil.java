/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.testutils.secure;

import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import static java.lang.System.lineSeparator;

/**
 * Util class for working with test key & trust stores.
 */
final class KeyStoreUtil {
  private KeyStoreUtil() {
  }

  /**
   * Write the supplied store to a temporary file.
   * @param name                 the name of the store being written.
   * @param base64EncodedStore   the base64 encode store content.
   * @return the path to the
   */
  static Path createTemporaryStore(final String name, final String base64EncodedStore) {
    try {
      final byte[] decoded = Base64.getDecoder().decode(base64EncodedStore);

      final File tempFile = TestUtils.tempFile();

      Files.write(tempFile.toPath(), decoded);

      final Path path = tempFile.toPath();
      System.out.println("Wrote temporary " + name + " for testing: " + path);
      return path;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create temporary store", e);
    }
  }

  public static void main(final String[] args) throws Exception {
    final Path path = Paths.get(args[0]);
    byte[] data = Files.readAllBytes(path);
    final byte[] encoded = Base64.getEncoder().encode(data);
    final String s = new String(encoded, StandardCharsets.UTF_8);

    System.out.println(String.format("Base64 encode content:%s"
                       + "=======================%s"
                       + "%s%s"
                       + "=======================",
                       lineSeparator(), lineSeparator(),
                       s, lineSeparator()));
  }
}
