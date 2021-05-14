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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.ApiJsonMapper;
import java.io.IOException;
import org.junit.Test;

public class KsqlErrorMessageTest {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private static final KsqlErrorMessage MESSAGE =
      new KsqlErrorMessage(40301, "foo");
  private static final String SERIALIZED_MESSAGE =
      "{\"@type\":\"generic_error\",\"error_code\":40301,\"message\":\"foo\"}";

  @Test
  public void shouldSerializeToJson() {
    // When:
    final String jsonMessage = serialize(MESSAGE);

    // Then:
    assertThat(jsonMessage, is(SERIALIZED_MESSAGE));
  }

  @Test
  public void shouldDeserializeFromJson() {
    // When:
    final KsqlErrorMessage message = deserialize(SERIALIZED_MESSAGE);

    // Then:
    assertThat(message, is(MESSAGE));
  }

  private static String serialize(final KsqlErrorMessage errorMessage) {
    try {
      return OBJECT_MAPPER.writeValueAsString(errorMessage);
    } catch (final IOException e) {
      throw new RuntimeException("test invalid", e);
    }
  }

  private static KsqlErrorMessage deserialize(final String json) {
    try {
      return OBJECT_MAPPER.readValue(json, KsqlErrorMessage.class);
    } catch (final IOException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }
}