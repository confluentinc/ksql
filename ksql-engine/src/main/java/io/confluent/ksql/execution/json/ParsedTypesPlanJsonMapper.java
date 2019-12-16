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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.parser.json.KsqlParserSerializationModule;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;

public final class ParsedTypesPlanJsonMapper {
  private ParsedTypesPlanJsonMapper() {
  }

  /**
   * Create an ObjectMapper configured for serializing/deserializing a KSQL physical plan.
   *
   * @return ObjectMapper instance
   */
  public static ObjectMapper create() {
    ObjectMapper mapper = PlanJsonMapper.create();
    mapper.registerModules(
        new KsqlParserSerializationModule(),
        new KsqlTypesSerializationModule(),
        new KsqlTypesDeserializationModule(true)
    );
    return mapper;
  }
}
