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

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.json.StructSerializationModule;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;

/**
 * Json mapper for the test framework.
 */
public enum TestJsonMapper {

  INSTANCE;

  private final ObjectMapper mapper = new ObjectMapper()
      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      .registerModule(new Jdk8Module())
      .registerModule(new JavaTimeModule())
      .registerModule(new StructSerializationModule())
      .registerModule(new KsqlTypesSerializationModule())
      .registerModule(new KsqlTypesDeserializationModule())
      .enable(Feature.ALLOW_COMMENTS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
      .setSerializationInclusion(Include.NON_EMPTY);

  public ObjectMapper get() {
    return mapper.copy();
  }
}