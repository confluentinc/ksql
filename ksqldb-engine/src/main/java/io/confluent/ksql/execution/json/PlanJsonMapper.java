/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.json;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.parser.json.KsqlParserSerializationModule;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;

/**
 * The Json mapper used for serializing and deserializing to internal topics such as the command and
 * config topics.
 */
public enum PlanJsonMapper {

  INSTANCE;

  private final ObjectMapper mapper = new ObjectMapper()
      .registerModules(
          new Jdk8Module(),
          new JavaTimeModule(),
          new KsqlParserSerializationModule(),
          new KsqlTypesSerializationModule(),
          new KsqlTypesDeserializationModule()
      )
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
      .enable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE)
      .setSerializationInclusion(Include.NON_NULL);

  public ObjectMapper get() {
    return mapper.copy();
  }
}
