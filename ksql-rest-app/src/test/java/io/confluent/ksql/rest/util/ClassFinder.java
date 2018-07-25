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

package io.confluent.ksql.rest.util;

import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ClassFinder {

  private static final String CLASS_FILE_EXT = ".class";
  private static final PathMatcher CLASS_MATCHER =
      FileSystems.getDefault().getPathMatcher("glob:**" + CLASS_FILE_EXT);

  private ClassFinder() {
  }

  public static List<Class<?>> getClasses(final String packageName) {
    try {
      final ClassLoader classLoader = ClassFinder.class.getClassLoader();
      final String path = packageName.replace('.', '/');
      final Enumeration<URL> resources = classLoader.getResources(path);
      final List<Path> dirs = new ArrayList<>();
      while (resources.hasMoreElements()) {
        final URL resource = resources.nextElement();
        if (!resource.toString().contains("/test-classes/")) {
          dirs.add(Paths.get(resource.getFile()));
        }
      }

      return dirs.stream()
          .flatMap(dir -> findClasses(dir, packageName).stream())
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new AssertionError("Failed to load classes in package: " + packageName, e);
    }
  }

  private static List<Class<?>> findClasses(final Path directory, final String packageName) {
    if (!directory.toFile().exists()) {
      return Collections.emptyList();
    }

    try (final Stream<Path> paths = Files.walk(directory)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(CLASS_MATCHER::matches)
          .map(path -> parseClass(packageName, path))
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new AssertionError("Failed to load classes in directory: " + directory, e);
    }
  }

  private static Class<?> parseClass(final String packageName, final Path path) {
    try {
      final String name = path.getFileName().toString();
      final String className = name.substring(0, name.length() - CLASS_FILE_EXT.length());
      return Class.forName(packageName + '.' + className);
    } catch (final Exception e) {
      throw new RuntimeException(
          "Failed to get class. packageName:" + packageName + ", path:" + path, e);
    }
  }
}
