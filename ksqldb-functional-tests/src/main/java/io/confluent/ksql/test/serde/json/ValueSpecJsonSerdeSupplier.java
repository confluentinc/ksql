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

package io.confluent.ksql.test.serde.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.test.serde.SerdeSupplier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ValueSpecJsonSerdeSupplier implements SerdeSupplier<Object> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  private static final ObjectMapper FLOAT_MAPPER = new ObjectMapper();

  private final ObjectMapper mapper;

  public ValueSpecJsonSerdeSupplier(
      final Map<String, Object> properties
  ) {
    mapper = (boolean) (properties.getOrDefault("use.exact.numeric.comparison", true))
        ? MAPPER : FLOAT_MAPPER;
  }

  @Override
  public Serializer<Object> getSerializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new ValueSpecJsonSerializer();
  }

  @Override
  public Deserializer<Object> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new ValueSpecJsonDeserializer();
  }

  private final class ValueSpecJsonSerializer implements Serializer<Object> {
    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public byte[] serialize(final String topicName, final Object spec) {
      if (spec == null) {
        return null;
      }
      try {
        final Object toSerialize = Converter.toJsonNode(spec);
        final byte[] bytes = mapper.writeValueAsBytes(toSerialize);
        return bytes;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final class ValueSpecJsonDeserializer implements Deserializer<Object> {
    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public Object deserialize(final String topicName, final byte[] data) {
      if (data == null) {
        return null;
      }
      try {
        return mapper.readValue(data, Object.class);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class Converter<T> {

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory
        .withExactBigDecimals(true);

    private static final List<Converter<?>> CONVERTORS = ImmutableList.of(
        Converter.converter(Boolean.class, JSON_NODE_FACTORY::booleanNode),
        Converter.converter(Integer.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(Long.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(Float.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(Double.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(BigDecimal.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(BigInteger.class, JSON_NODE_FACTORY::numberNode),
        Converter.converter(String.class, JSON_NODE_FACTORY::textNode),
        Converter.converter(Collection.class, Converter::handleCollection),
        Converter.converter(Map.class, Converter::handleMap)
    );

    private final Class<T> type;
    private final Function<T, JsonNode> mapper;

    static JsonNode toJsonNode(final Object obj) {
      if (obj == null) {
        return JSON_NODE_FACTORY.nullNode();
      }

      final List<Converter<?>> candidates = CONVERTORS.stream()
          .filter(c -> c.handles(obj))
          .collect(Collectors.toList());

      if (candidates.isEmpty()) {
        throw new UnsupportedOperationException("Test framework does not current handle " + obj);
      }

      if (candidates.size() > 1) {
        throw new RuntimeException("Ambiguous handling of " + obj);
      }

      return candidates.get(0).convert(obj);
    }

    private boolean handles(final Object obj) {
      return type.isAssignableFrom(obj.getClass());
    }

    private static <T> Converter<T> converter(
        final Class<T> type,
        final Function<T, JsonNode> mapper
    ) {
      return new Converter<>(type, mapper);
    }

    private Converter(
        final Class<T> type,
        final Function<T, JsonNode> mapper
    ) {
      this.type = Objects.requireNonNull(type, "type");
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    private JsonNode convert(final Object o) {
      final T cast = type.cast(o);
      return mapper.apply(cast);
    }

    private static JsonNode handleCollection(final Collection<?> collection) {
      final ArrayNode list = JSON_NODE_FACTORY.arrayNode();
      for (final Object element : collection) {
        list.add(toJsonNode(element));
      }
      return list;
    }

    private static JsonNode handleMap(final Map<?, ?> map) {
      final ObjectNode node = JSON_NODE_FACTORY.objectNode();

      if (map.isEmpty()) {
        return node;
      }

      map.forEach((k, v) -> {
        if (!(k instanceof String)) {
          throw new UnsupportedOperationException(
              "Test framework does not yet support maps with non-string keys");
        }

        node.set((String) k, toJsonNode(v));
      });

      return node;
    }
  }

}