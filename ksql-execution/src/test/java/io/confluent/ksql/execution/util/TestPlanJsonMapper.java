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

package io.confluent.ksql.execution.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestPlanJsonMapper {
  private final Map<Object, String> registeredMappings = new HashMap<>();
  private final Map<String, Object> reverseMappings = new HashMap<>();
  private final ObjectMapper mapper;

  public TestPlanJsonMapper() {
    mapper = PlanJsonMapper.create();
    mapper.registerModule(new RegisteredSerializationModule());
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  public void register(final Object object, final String serialized) {
    registeredMappings.put(object, serialized);
    reverseMappings.put(serialized, object);
  }

  private class RegisteredSerializationModule extends SimpleModule {
    RegisteredSerializationModule() {
      super();
      addFor(LogicalSchema.class);
      addFor(Expression.class);
      addFor(ColumnRef.class);
      addFor(FunctionCall.class);
      addFor(SelectExpression.class);
      addFor(KsqlWindowExpression.class);
      addFor(SqlType.class);
    }

    private void addFor(final Class<?> clazz) {
      addSerializer(clazz, new RegisteredSerializer<>());
      addDeserializer(clazz, new RegisteredDeserializer<>());
    }
  }

  private class RegisteredSerializer<T> extends JsonSerializer<T> {
    @Override
    public void serialize(
        final T obj,
        final JsonGenerator gen,
        final SerializerProvider serializerProvider
    ) throws IOException {
      if (!registeredMappings.containsKey(obj)) {
        throw new IllegalStateException("No registered mapping for " + obj);
      }
      final String serialized = registeredMappings.get(obj);
      gen.writeString(serialized);
    }
  }

  private class RegisteredDeserializer<T> extends JsonDeserializer<T> {
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(
        final JsonParser parser,
        final DeserializationContext ctxt
    ) throws IOException {
      final String text = parser.readValueAs(String.class);
      if (!reverseMappings.containsKey(text)) {
        throw new IllegalStateException("No reverse mapping for " + text);
      }
      return (T) reverseMappings.get(text);
    }
  }
}
