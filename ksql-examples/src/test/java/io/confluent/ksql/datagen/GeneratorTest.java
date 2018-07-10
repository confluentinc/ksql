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

package io.confluent.ksql.datagen;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.avro.random.generator.Generator;

import static io.confluent.ksql.datagen.util.ResourceUtil.getResourceRoot;
import static io.confluent.ksql.datagen.util.ResourceUtil.loadContent;

@RunWith(Parameterized.class)
public class GeneratorTest {

  private static final Random RNG = new Random();
  private final Path fileName;
  private final String content;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return findTestSchemas()
        .map(fileName -> new Object[]{fileName, loadContent(fileName)})
        .collect(Collectors.toCollection(ArrayList::new));
  }

  public GeneratorTest(final Path fileName, final String content) {
    this.fileName = fileName;
    this.content = content;
  }

  @Test
  public void shouldHandleSchema() {
    final Generator generator = new Generator(content, RNG);
    final Object generated = generator.generate();
    System.out.println(fileName + ": " + generated);
  }

  private static Stream<Path> findTestSchemas() {
    try {
      final Path resourceRoot = getResourceRoot();
      return Files.list(resourceRoot)
          .filter(path -> path.toString().endsWith(".avro"));

    } catch (final Exception e) {
      throw new RuntimeException("failed to find test schemas", e);
    }
  }
}
