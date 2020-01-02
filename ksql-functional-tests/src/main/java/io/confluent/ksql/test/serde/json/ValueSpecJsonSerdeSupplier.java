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
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  @Override
  public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return new ValueSpecJsonSerializer();
  }

  @Override
  public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return new ValueSpecJsonDeserializer();
  }

  private static final class ValueSpecJsonSerializer implements Serializer<Object> {
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
        return MAPPER.writeValueAsBytes(toSerialize);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class ValueSpecJsonDeserializer implements Deserializer<Object> {
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
        return MAPPER.readValue(data, Object.class);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class Converter<T> {

    private static final List<Converter<?>> CONVERTORS = ImmutableList.of(
        Converter.converter(Boolean.class, JsonNodeFactory.instance::booleanNode),
        Converter.converter(Integer.class, JsonNodeFactory.instance::numberNode),
        Converter.converter(Long.class, JsonNodeFactory.instance::numberNode),
        Converter.converter(Float.class, JsonNodeFactory.instance::numberNode),
        Converter.converter(Double.class, JsonNodeFactory.instance::numberNode),
        Converter.converter(BigDecimal.class, JsonNodeFactory.instance::numberNode),
        Converter.converter(String.class, JsonNodeFactory.instance::textNode),
        Converter.converter(Collection.class, Converter::handleCollection),
        Converter.converter(Map.class, Converter::handleMap)
    );

    private final Class<T> type;
    private final Function<T, JsonNode> mapper;

    static JsonNode toJsonNode(final Object obj) {
      if (obj == null) {
        return JsonNodeFactory.instance.nullNode();
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
      final ArrayNode list = JsonNodeFactory.instance.arrayNode();
      for (final Object element : collection) {
        list.add(toJsonNode(element));
      }
      return list;
    }

    private static JsonNode handleMap(final Map<?, ?> map) {
      final ObjectNode node = JsonNodeFactory.instance.objectNode();

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