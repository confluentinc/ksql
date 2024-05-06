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

package io.confluent.ksql.parser.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import java.io.IOException;

class KsqlWindowExpressionSerializer extends JsonSerializer<KsqlWindowExpression> {
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
  }

  @Override
  public void serialize(
      final KsqlWindowExpression expression,
      final JsonGenerator jsonGenerator,
      final SerializerProvider serializerProvider
  ) throws IOException {
    jsonGenerator.writeObject(mapper.valueToTree(expression));
  }
}
