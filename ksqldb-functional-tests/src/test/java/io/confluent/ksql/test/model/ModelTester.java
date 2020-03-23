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

import static io.confluent.ksql.test.loader.JsonTestLoader.OBJECT_MAPPER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Helper for proving model classes (de)serialize properly
 */
public final class ModelTester {

  private ModelTester() {
  }

  public static void assertRoundTrip(final Object original) {
    try {
      // When:
      final String json = OBJECT_MAPPER.writeValueAsString(original);
      final Object deserialized = OBJECT_MAPPER.readValue(json, original.getClass());

      // Then:
      final String reSerialized = OBJECT_MAPPER.writeValueAsString(deserialized);
      assertThat(reSerialized, is(json));
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
    }
  }
}
