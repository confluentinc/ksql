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
package io.confluent.ksql.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestExecutionListener;
import io.confluent.ksql.test.tools.TestExecutor;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

final class EndToEndEngineTestUtil {

  private EndToEndEngineTestUtil(){}

  static void shouldBuildAndExecuteQuery(final TestCase testCase) {
    try (final TestExecutor testExecutor = TestExecutor.create(true, Optional.empty())) {
      testExecutor.buildAndExecuteQuery(testCase, TestExecutionListener.noOp());
    } catch (final AssertionError | Exception e) {
      throw new AssertionError(e.getMessage()
          + System.lineSeparator()
          + "failed test: " + testCase.getName()
          + System.lineSeparator()
          + "in " + testCase.getTestLocation(),
          e
      );
    }
  }

  @SuppressWarnings("unchecked")
  static Object avroToValueSpec(final Object avro,
      final org.apache.avro.Schema schema,
      final boolean toUpper) {
    if (avro == null) {
      return null;
    }

    switch (schema.getType()) {
      case INT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return avro;
      case LONG:
        // Ensure that smaller long values match the value spec from the test file.
        // The json deserializer uses Integer for any number less than Integer.MAX_VALUE.
        if (((Long)avro) < Integer.MAX_VALUE && ((Long)avro) > Integer.MIN_VALUE) {
          return ((Long)avro).intValue();
        }
        return avro;
      case ENUM:
      case STRING:
        return avro.toString();
      case ARRAY:
        if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME) ||
            Objects.equals(
                schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                AvroData.MAP_ENTRY_TYPE_NAME)
        ) {
          final org.apache.avro.Schema valueSchema
              = schema.getElementType().getField("value").schema();
          return ((List) avro).stream().collect(
              Collectors.toMap(
                  m -> ((GenericData.Record) m).get("key").toString(),
                  m -> (avroToValueSpec(((GenericData.Record) m).get("value"), valueSchema, toUpper))
              )
          );
        }
        return ((List)avro).stream()
            .map(o -> avroToValueSpec(o, schema.getElementType(), toUpper))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)avro).entrySet().stream().collect(
            Collectors.toMap(
                e -> e.getKey().toString(),
                e -> avroToValueSpec(e.getValue(), schema.getValueType(), toUpper)
            )
        );
      case RECORD:
        final Map<String, Object> recordSpec = new HashMap<>();
        schema.getFields().forEach(
            f -> recordSpec.put(
                toUpper ? f.name().toUpperCase() : f.name(),
                avroToValueSpec(
                    ((GenericData.Record)avro).get(f.name()),
                    f.schema(),
                    toUpper)
            )
        );
        return recordSpec;
      case UNION:
        final int pos = GenericData.get().resolveUnion(schema, avro);
        final boolean hasNull = schema.getTypes().stream()
            .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
        final Object resolved = avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
        if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
            || schema.getTypes().size() == 2 && hasNull) {
          return resolved;
        }
        final Map<String, Object> ret = Maps.newHashMap();
        schema.getTypes()
            .forEach(
                s -> ret.put(toUpper ? s.getName().toUpperCase() : s.getName(), null));
        final String name = schema.getTypes().get(pos).getName();
        ret.put(toUpper ? name.toUpperCase() : name, resolved);
        return ret;
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }

  @SuppressWarnings("unchecked")
  static JsonNode avroToJson(
      final Object avro,
      final org.apache.avro.Schema schema,
      final boolean toUpper
  ) {
    if (avro == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    switch (schema.getType()) {
      case INT:
        return JsonNodeFactory.instance.numberNode((int) avro);
      case LONG:
        return JsonNodeFactory.instance.numberNode((long) avro);
      case FLOAT:
        return JsonNodeFactory.instance.numberNode((float) avro);
      case DOUBLE:
        return JsonNodeFactory.instance.numberNode((double) avro);
      case BOOLEAN:
        return JsonNodeFactory.instance.booleanNode((boolean) avro);
      case ENUM:
      case STRING:
        return JsonNodeFactory.instance.textNode(avro.toString());
      case ARRAY:
        if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME) ||
            Objects.equals(
                schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                AvroData.MAP_ENTRY_TYPE_NAME)
            ) {
          final org.apache.avro.Schema valueSchema
              = schema.getElementType().getField("value").schema();

          final ObjectNode node = JsonNodeFactory.instance.objectNode();
          ((List) avro).forEach(m -> {
            final GenericData.Record record = (Record) m;
            node.set(
                record.get("key").toString(),
                avroToJson(record.get("value"), valueSchema, toUpper));
          });

          return node;
        }

        final ArrayNode array = JsonNodeFactory.instance.arrayNode();
        ((List) avro).stream()
            .map(o -> avroToJson(o, schema.getElementType(), toUpper))
            .forEach(j -> array.add((JsonNode) j));
        return array;
      case MAP:
        final ObjectNode map = JsonNodeFactory.instance.objectNode();
        ((Map<Object, Object>) avro).forEach(
            (k, v) -> map.set(
                k.toString(),
                avroToJson(v, schema.getValueType(), toUpper)));
        return map;
      case RECORD:
        final ObjectNode record = JsonNodeFactory.instance.objectNode();
        schema.getFields().forEach(
            f -> record.set(
                toUpper ? f.name().toUpperCase() : f.name(),
                avroToJson(
                    ((GenericData.Record)avro).get(f.name()),
                    f.schema(),
                    toUpper)
            )
        );
        return record;
      case UNION:
        final int pos = GenericData.get().resolveUnion(schema, avro);
        final boolean hasNull = schema.getTypes().stream()
            .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
        final JsonNode resolved = avroToJson(avro, schema.getTypes().get(pos), toUpper);
        if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
            || schema.getTypes().size() == 2 && hasNull) {
          return resolved;
        }

        final ObjectNode ret = JsonNodeFactory.instance.objectNode();
        schema.getTypes().forEach(
            s -> ret.set(toUpper ? s.getName().toUpperCase() : s.getName(), null));
        final String name = schema.getTypes().get(pos).getName();
        ret.set(toUpper ? name.toUpperCase() : name, resolved);
        return ret;
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  static String buildTestName(
      final Path testPath,
      final String testName,
      final String postfix
  ) {
    final String fileName = com.google.common.io.Files.getNameWithoutExtension(testPath.toString());
    final String pf = postfix.isEmpty() ? "" : " - " + postfix;
    return fileName + " - " + testName + pf;
  }

}