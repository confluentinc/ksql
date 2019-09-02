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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestExecutor;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;

public final class EndToEndEngineTestUtil {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private EndToEndEngineTestUtil(){}

  public static void shouldBuildAndExecuteQuery(final TestCase testCase) {

    try (final TestExecutor testExecutor = new TestExecutor()) {
      testExecutor.buildAndExecuteQuery(testCase);
    } catch (final RuntimeException e) {
      testCase.handleException(e);
    } catch (final AssertionError e) {
      throw new AssertionError("test: " + testCase.getName() + System.lineSeparator()
          + "file: " + testCase.getTestFile()
          + "Failed with error:" + e.getMessage(), e);
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
                s -> ret.put(s.getName().toUpperCase(), null));
        ret.put(schema.getTypes().get(pos).getName().toUpperCase(), resolved);
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

  static Optional<org.apache.avro.Schema> buildAvroSchema(final JsonNode schema) {
    if (schema instanceof NullNode) {
      return Optional.empty();
    }

    try {
      final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
      final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
      return Optional.of(parser.parse(schemaString));
    } catch (final Exception e) {
      throw new InvalidFieldException("schema", "failed to parse", e);
    }
  }
}