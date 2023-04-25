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

package io.confluent.ksql.function.udf.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Shared Object mapper used by JSON processing UDFs
 */
public enum UdfJsonMapper {

  INSTANCE;

  private final ObjectMapper mapper = new ObjectMapper()
      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  public ObjectMapper get() {
    return mapper.copy();
  }
}