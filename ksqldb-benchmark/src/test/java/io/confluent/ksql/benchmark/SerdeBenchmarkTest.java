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

package io.confluent.ksql.benchmark;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.benchmark.SerdeBenchmark.SerdeState;
import java.util.List;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openjdk.jmh.annotations.Param;

@RunWith(Parameterized.class)
public class SerdeBenchmarkTest {

  private static final List<String> SCHEMAS = ImmutableList.of("impressions", "metrics");
  private static final List<String> FORMATS = ImmutableList.of("JSON", "Avro");
  private static final String TOPIC_NAME = "serde_benchmark";

  private final SerdeState serdeState = new SerdeState();

  public SerdeBenchmarkTest(final String params) {
    this.serdeState.params = Objects.requireNonNull(params, "params");
  }

  @Parameterized.Parameters(name = "{0}")
  public static String[] data() throws Exception {
    final Param paramAnnotation = SerdeState.class.getDeclaredField("params")
        .getAnnotation(Param.class);

    if (paramAnnotation == null) {
      throw new AssertionError("Invalid test: " + SerdeState.class.getSimpleName()
          + ".params missing @Param annotation");
    }

    return paramAnnotation.value();
  }

  @Before
  public void setUp() throws Exception {
    serdeState.setUp();
  }

  @Test
  public void shouldSerializeDeserialize() {
    assertThat(serdeState.serializer.serialize(TOPIC_NAME, serdeState.data), is(serdeState.bytes));
    assertThat(serdeState.deserializer.deserialize(TOPIC_NAME, serdeState.bytes),
        is(serdeState.data));
  }
}