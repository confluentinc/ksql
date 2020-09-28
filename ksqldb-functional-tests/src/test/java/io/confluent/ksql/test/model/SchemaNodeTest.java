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
import org.junit.Test;

public class SchemaNodeTest {

  private static final ObjectMapper OBJECT_MAPPER = TestJsonMapper.INSTANCE.get();

  private static final SchemaNode INSTANCE = new SchemaNode(
    "Some Logical Schema",
      ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES),
      ImmutableSet.of(SerdeFeature.WRAP_SINGLES)
  );

  private static final SchemaNode NO_SERDE_FEATURES = new SchemaNode(
      "Some Logical Schema",
      ImmutableSet.of(),
      ImmutableSet.of()
  );

  @Test
  public void shouldRoundTrip() {
    ModelTester.assertRoundTrip(INSTANCE);
  }

  @Test
  public void shouldRoundTripWithoutFeatures() {
    ModelTester.assertRoundTrip(NO_SERDE_FEATURES);
  }

  @Test
  public void shouldReadLegacy() throws Exception {
    // Given:
    final String legacy = "{\n"
        + " \"schema\" : \"Some Schema\",\n"
        + " \"serdeOptions\" : [\"UNWRAP_SINGLE_VALUES\"]\n"
        + "}";

    // When:
    final Object deserialized = OBJECT_MAPPER.readValue(legacy, SchemaNode.class);

    // Then:
    assertThat(deserialized, is(new SchemaNode(
        "Some Schema",
        ImmutableSet.of(),
        ImmutableSet.of(SerdeFeature.UNWRAP_SINGLES)
    )));
  }
}