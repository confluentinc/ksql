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

package io.confluent.ksql.metastore;

import io.confluent.ksql.execution.expression.tree.BytesLiteral;
import io.confluent.ksql.test.util.ImmutableTester;
import java.util.Collection;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test that any type annotated as immutable _is_ immutable
 *
 * <p>Note: each module has a copy of this test in a unique package to ensure the classes in that
 * module are correctly annotated.
 *
 * <p>Running any of these tests in IntelliJ will test most, if not all, the classes.
 *
 * <p>Running in via Maven varies depending on the target. While the {@code test} target
 * detects classes both from the current module and any module it depends on, the {@code install}
 * target only detects classes within the current module.
 *
 * <p>The build server runs the {@code install} target. Hence the need for each module to have a
 * copy of this test.
 *
 * <p>Having each test in a unique package ensures the build server doesn't overwrite one test's
 * output with another.
 *
 * there are multiple copies of this test to ensure all types are tested.
 * However, this does mean some times are tested multiple types.
 */
@RunWith(Parameterized.class)
public class ImmutabilityTest {

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ImmutableTester
        .classesMarkedImmutable("io.confluent.ksql");
  }

  public ImmutabilityTest(final Class<?> modelClass) {
    this.modelClass = Objects.requireNonNull(modelClass, "modelClass");
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(JoinWindows.class)
        .withKnownImmutableType(ConnectSchema.class)
        .withKnownImmutableType(KStream.class)
        .withKnownImmutableType(KTable.class)
        .withKnownImmutableType(KGroupedStream.class)
        .withKnownImmutableType(KGroupedTable.class)
        .withKnownImmutableType(Serde.class)
        .withKnownImmutableType(BytesLiteral.class)
        .test(modelClass);
  }
}
