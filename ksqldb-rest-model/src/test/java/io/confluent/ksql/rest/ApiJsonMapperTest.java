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

package io.confluent.ksql.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DecimalNode;
import java.math.BigDecimal;
import java.sql.Timestamp;
import org.junit.Test;

public class ApiJsonMapperTest {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  @Test
  public void shouldNotAutoCloseTarget() {
    assertThat(OBJECT_MAPPER.isEnabled(JsonGenerator.Feature.AUTO_CLOSE_TARGET), is(false));
  }

  @Test
  public void shouldIgnoreUnknownProperties() {
    assertThat(OBJECT_MAPPER.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),
        is(false));
  }

  @Test
  public void shouldSerializeDecimalsWithoutLossOfTrailingZeros() throws Exception {
    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(new BigDecimal("10.0"));

    // Then:
    assertThat(json, is("10.0"));
  }

  @Test
  public void shouldDeserializeDecimalsWithoutLossOfTrailingZeros() throws Exception {
    // When:
    final JsonNode node = OBJECT_MAPPER.readTree("10.0");

    // Then:
    assertThat(node, is(instanceOf(DecimalNode.class)));
    assertThat(node.decimalValue(), is(new BigDecimal("10.0")));
  }

  @Test
  public void shouldNotUseScientificNotationWhenSerializingDecimals() throws Exception {
    // When:
    final String result = OBJECT_MAPPER.writeValueAsString(new BigDecimal("1e+1"));

    // Then:
    assertThat(result, is("10"));
  }

  @Test
  public void shouldFormatTimestampsToISO8601() throws Exception {
    // When:
    final String result = OBJECT_MAPPER.writeValueAsString(new Timestamp(943959600000L));

    // Then:
    assertThat(result, is("\"1999-11-30T11:00:00.000\""));
  }
}