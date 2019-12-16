/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.util;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.plan.ExecutionStep;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class TestExecutionStepSerializationUtil {
  private TestExecutionStepSerializationUtil() {
  }

  private static String loadCase(final Class<?> testClass, final String name) throws IOException {
    final Path base = Paths.get("src/test/resources/formats");
    final Path test = base.resolve(testClass.getSimpleName()).resolve(name);
    return new String(Files.readAllBytes(test), Charset.defaultCharset());
  }

  public static void shouldSerialize(
      final String testCase,
      final Object object,
      final ObjectMapper mapper) throws IOException {
    // Given:
    final String serialized = loadCase(object.getClass(), testCase);

    // Then:
    assertThat(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object), is(serialized));
    assertThat(mapper.readValue(serialized, object.getClass()), is(object));
  }
}
