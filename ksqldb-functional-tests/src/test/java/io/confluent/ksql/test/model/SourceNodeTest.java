/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.test.tools.TestJsonMapper;
import java.util.Optional;
import org.junit.Test;

public class SourceNodeTest {

  private static final ObjectMapper OBJECT_MAPPER = TestJsonMapper.INSTANCE.get();

  static final SourceNode INSTANCE = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.of(ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES)),
      Optional.of(ImmutableSet.of(SerdeFeature.WRAP_SINGLES))
  );

  private static final SourceNode INSTANCE_WITHOUT_SERDE_FEATURES = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.empty(),
      Optional.empty()
  );

  private static final SourceNode INSTANCE_WITH_EMPTY_SERDE_FEATURES = new SourceNode(
      "bob",
      "stream",
      Optional.of("ROWKEY INT KEY, NAME STRING"),
      Optional.of(KeyFormatNodeTest.INSTANCE),
      Optional.of("JSON"),
      Optional.of(ImmutableSet.of()),
      Optional.of(ImmutableSet.of())
  );

  @Test
  public void shouldRoundTrip() {
    ModelTester.assertRoundTrip(INSTANCE);
  }

  @Test
  public void shouldRoundTripWithoutSerdeFeatures() {
    ModelTester.assertRoundTrip(INSTANCE_WITHOUT_SERDE_FEATURES);
  }

  @Test
  public void shouldRoundTripWithEmptySerdeFeatures() {
    ModelTester.assertRoundTrip(INSTANCE_WITH_EMPTY_SERDE_FEATURES);
  }

  @Test
  public void shouldReadLegacy() throws Exception {
    // Given:
    final String legacy = "{"
        + "\"name\": \"OUTPUT\", "
        + "\"type\": \"stream\", "
        + "\"schema\": \"ROWKEY INT KEY, `C1` INT\","
        + "\"serdeOptions\": [\"UNWRAP_SINGLE_VALUES\"]"
        + "}";

    // When:
    final Object deserialized = OBJECT_MAPPER.readValue(legacy, SourceNode.class);

    // Then:
    assertThat(deserialized, is(new SourceNode(
        "OUTPUT",
        "stream",
        Optional.of("ROWKEY INT KEY, `C1` INT"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES))
    )));
  }
}