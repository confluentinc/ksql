/*
 * Copyright 2019 Confluent Inc.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class PlanJsonMapper {
  private PlanJsonMapper() {
  }

  /**
   * Create an ObjectMapper configured for serializing/deserializing a KSQL physical plan.
   *
   * @return ObjectMapper instance
   */
  public static ObjectMapper create() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModules(
        new Jdk8Module(),
        new JavaTimeModule()
    );
    mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    mapper.enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);
    mapper.enable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
    return mapper;
  }
}
