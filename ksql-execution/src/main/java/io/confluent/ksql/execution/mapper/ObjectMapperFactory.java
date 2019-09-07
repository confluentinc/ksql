package io.confluent.ksql.execution.mapper;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;

public final class ObjectMapperFactory {
  private ObjectMapperFactory() {
  }

  public static ObjectMapper build(
      final JsonSerializer<Expression> expressionSerializer,
      final JsonDeserializer<Expression> expressionDeserializer) {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper
        .configure(MapperFeature.AUTO_DETECT_CREATORS, false)
        .configure(MapperFeature.AUTO_DETECT_FIELDS, false)
        .configure(MapperFeature.AUTO_DETECT_GETTERS, false)
        .configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false)
        .configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new ExpressionModule(expressionSerializer, expressionDeserializer));
    return objectMapper;
  }

  private static class ExpressionModule extends SimpleModule {
    @SuppressWarnings("unchecked")
    ExpressionModule(
        final JsonSerializer<Expression> serializer,
        final JsonDeserializer<Expression> deserializer) {
      addSerializer(Expression.class, serializer);
      addDeserializer(Expression.class, deserializer);
      addDeserializer(FunctionCall.class, (JsonDeserializer) deserializer);
    }
  }
}
