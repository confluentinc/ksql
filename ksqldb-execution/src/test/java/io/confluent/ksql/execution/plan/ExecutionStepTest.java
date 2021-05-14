/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.test.util.ClassFinder;
import io.confluent.ksql.test.util.ImmutableTester;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ExecutionStepTest {

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(ExecutionStep.class.getPackage().getName())
        .stream()
        .filter(ExecutionStep.class::isAssignableFrom)
        .collect(Collectors.toList());
  }

  public ExecutionStepTest(final Class<?> modelClass) {
    this.modelClass = modelClass;
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(Struct.class) // Not truly immutable, but soon to be replaced...
        .test(modelClass);
  }
}
