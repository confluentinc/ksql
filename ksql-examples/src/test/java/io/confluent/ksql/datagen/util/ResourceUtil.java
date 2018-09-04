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

package io.confluent.ksql.datagen.util;

import io.confluent.ksql.datagen.GeneratorTest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Collectors;

public final class ResourceUtil {

  private ResourceUtil() {
  }

  public static String loadContent(final Path filePath) {
    try {
      return Files.lines(filePath, StandardCharsets.UTF_8)
          .collect(Collectors.joining("\n"));
    } catch (final IOException ioe) {
      throw new RuntimeException("failed to find test test-schema " + filePath, ioe);
    }
  }

  @SuppressWarnings("ConstantConditions")
  public static Path getResourceRoot() throws IOException, URISyntaxException {
    final URI uri = GeneratorTest.class.getClassLoader().getResource("product.avro").toURI();
    if ("jar".equals(uri.getScheme())) {
      final FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap(), null);
      return fileSystem.getPath("path/to/folder/inside/jar").getParent();
    } else {
      return Paths.get(uri).getParent();
    }
  }
}
