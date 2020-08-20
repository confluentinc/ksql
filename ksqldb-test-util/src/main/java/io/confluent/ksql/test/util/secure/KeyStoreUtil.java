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

package io.confluent.ksql.test.util.secure;

import static java.lang.System.lineSeparator;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.kafka.test.TestUtils;

/**
 * Util class for working with test key & trust stores.
 */
public final class KeyStoreUtil {
  private KeyStoreUtil() {
  }

  /**
   * Write the supplied store to a temporary file.
   * @param name                 the name of the store being written.
   * @param base64EncodedStore   the base64 encoded store content.
   * @return the path of the temporary file
   */
  static Path createTemporaryStore(final String name, final String base64EncodedStore) {
    try {
      final File tempFile = TestUtils.tempFile();
      final Path path = tempFile.toPath();

      putStore(path, base64EncodedStore);

      System.out.println("Wrote temporary " + name + " for testing: " + path);
      return path;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to create temporary store", e);
    }
  }

  /**
   * Write the supplied store to the supplied path.
   * @param path                the path to write to.
   * @param base64EncodedStore  the base64 encoded store content.
   */
  public static void putStore(final Path path, final String base64EncodedStore) {
    try {
      final byte[] decoded = Base64.getDecoder().decode(base64EncodedStore);
      Files.write(path, decoded);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to put store", e);
    }
  }

  public static void main(final String[] args) throws Exception {
    final Path path = Paths.get(args[0]);
    final byte[] data = Files.readAllBytes(path);
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
