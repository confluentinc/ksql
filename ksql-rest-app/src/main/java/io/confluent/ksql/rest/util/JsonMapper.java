/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.entity.SchemaMapper;

public enum JsonMapper {
  INSTANCE;

  public final ObjectMapper mapper =
      new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

  JsonMapper() {
    new SchemaMapper().registerToObjectMapper(mapper);
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new StructSerializationModule());
  }
}