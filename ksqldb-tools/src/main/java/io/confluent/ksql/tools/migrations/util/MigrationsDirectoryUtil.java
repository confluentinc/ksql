/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.util;

import io.confluent.ksql.tools.migrations.MigrationException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;

public class MigrationsDirectoryUtil {
  private MigrationsDirectoryUtil() {
  }

  public static String getFileNameForVersion(final String version, final String path) {
    final String prefix = "V" + StringUtils.leftPad(version, 6, "0");
    final File directory = new File(path);
    for (String name : directory.list()) {
      if (name.startsWith(prefix)) {
        return name;
      }
    }
    return null;
  }

  public static String getFileForVersion(final String version, final String path) {
    final String filename = getFileNameForVersion(version, path);
    if (filename == null) {
      throw new MigrationException("Cannot find migration file with version "
          + version + " in " + path);
    }
    final String filepath = path + "/" + filename;
    try {
      return new String(Files.readAllBytes(Paths.get(filepath)), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new MigrationException(
          String.format("Failed to read %s: %s", filepath, e.getMessage()));
    }
  }
}
