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

package io.confluent.ksql.util;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class CliUtils {

  private static final Logger log = LogManager.getLogger(CliUtils.class);

  private CliUtils() {
  }

  public static boolean createFile(final Path path) {
    try {
      final Path parent = path.getParent();
      if (parent == null) {
        log.warn("Failed to create file as the parent was null. path: {}", path);
        return false;
      }
      Files.createDirectories(parent);
      if (Files.notExists(path)) {
        Files.createFile(path);
      }
      return true;
    } catch (final Exception e) {
      log.warn("createFile failed, path: {}", path, e);
      return false;
    }
  }
}
