/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.compress.utils.IOUtils;

public final class ZipUtil {

  private ZipUtil() {
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void unzip(final File sourceFile, final File outputDir) {
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new KsqlException("Failed to create output directory: " + outputDir);
    }

    try (ZipInputStream input = new ZipInputStream(new FileInputStream(sourceFile))) {

      ZipEntry entry;
      while ((entry = input.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }

        final File file = new File(outputDir, entry.getName());
        final File parent = file.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
          throw new KsqlException("Failed to create output directory: " + parent);
        }

        try (FileOutputStream output = new FileOutputStream(file)) {
          IOUtils.copy(input, output);
        } catch (final Exception e) {
          throw new RuntimeException("Error expanding entry '" + entry.getName() + "'", e);
        }
      }

      input.closeEntry();
    } catch (final IOException e) {
      throw new KsqlException(
          "Failed to unzip '" + sourceFile + "' into '" + outputDir + "'", e);
    }
  }
}
