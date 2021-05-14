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

package io.confluent.ksql.rest;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import java.math.BigDecimal;
import org.junit.Test;

public class GenericRowSerializationTest {

  private static final ObjectMapper MAPPER = ApiJsonMapper.INSTANCE.get();

  @Test
  public void shouldSerialize() throws Exception {
    // Given:
    final GenericRow original = genericRow(1, 2L, 3.0, Long.MAX_VALUE);

    // When:
    final String json = MAPPER.writeValueAsString(original);

    // Then:
    assertThat(json, is("{\"columns\":[1,2,3.0,9223372036854775807]}"));

    // When:
    final GenericRow result = MAPPER.readValue(json, GenericRow.class);

    // Then:
    assertThat(result, is(genericRow(
        1,
        2,                      // Note: int, not long, as JSON doesn't distinguish
        BigDecimal.valueOf(3.0),// Note: decimal, not double, as Jackson is configured this way
        Long.MAX_VALUE
    )));
  }
}
