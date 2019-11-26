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

package io.confluent.ksql;

import io.confluent.ksql.test.util.ImmutableTester;
import io.confluent.ksql.types.KsqlStruct;
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
 */
@RunWith(Parameterized.class)
public class ImmutabilityTest {

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ImmutableTester
        .classesMarkedImmutable(ImmutabilityTest.class.getPackage().getName());
  }

  public ImmutabilityTest(final Class<?> modelClass) {
    this.modelClass = Objects.requireNonNull(modelClass, "modelClass");
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(KsqlStruct.class)
        .withKnownImmutableType(JoinWindows.class)
        .withKnownImmutableType(ConnectSchema.class)
        .withKnownImmutableType(KStream.class)
        .withKnownImmutableType(KTable.class)
        .withKnownImmutableType(KGroupedStream.class)
        .withKnownImmutableType(KGroupedTable.class)
        .withKnownImmutableType(Serde.class)
        .test(modelClass);
  }
}
