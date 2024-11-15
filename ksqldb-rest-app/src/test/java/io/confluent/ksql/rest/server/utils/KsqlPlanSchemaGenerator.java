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

package io.confluent.ksql.rest.server.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import com.kjetland.jackson.jsonSchema.SubclassesResolver;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;


public final class KsqlPlanSchemaGenerator {

  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();

  private static final Map<Class<?>, JsonNode> POLYMORPHIC_TYPES;

  static {
    try {
      POLYMORPHIC_TYPES = ImmutableMap.of(
          ExecutionStep.class, definitionForPolymorphicType(ExecutionStep.class)
      );
    } catch (final JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  private KsqlPlanSchemaGenerator() {
  }

  private static JsonSchemaConfig configure() {
    final SubclassesResolver subclassesResolver = new SubclassesResolver(
        Collections.singletonList("io.confluent.ksql.rest.server.utils"),
        Collections.emptyList()
    );

    return JsonSchemaConfig.builder()
        .autoGenerateTitleForProperties(false)
        .defaultArrayFormat(null)
        .useOneOfForOption(false)
        .useOneOfForNullables(false)
        .usePropertyOrdering(false)
        .hidePolymorphismTypeProperty(false)
        .useMinLengthForNotNull(false)
        .useTypeIdForDefinitionName(false)
        .customType2FormatMapping(Collections.emptyMap())
        .useMultipleEditorSelectViaProperty(false)
        .uniqueItemClasses(new HashSet<>())
        .classTypeReMapping(
            new ImmutableMap.Builder<Class<?>, Class<?>>()
            .put(LogicalSchema.class, String.class)
            .put(SqlType.class, String.class)
            .put(SelectExpression.class, String.class)
            .put(Expression.class, String.class)
            .put(FunctionCall.class, String.class)
            .put(KsqlWindowExpression.class, String.class)
            .put(Duration.class, Long.class)
            .build()
        )
        .jsonSuppliers(Collections.emptyMap())
        .jsonSchemaDraft(JsonSchemaDraft.DRAFT_04)
        .failOnUnknownProperties(true)
        .subclassesResolver(subclassesResolver)
        .build();
  }

  private static JsonNode generate(final Class<?> clazz) throws JsonMappingException {
    final JsonSchemaGenerator generator = new JsonSchemaGenerator(MAPPER, configure());
    return generator.generateJsonSchema(clazz);
  }

  private static JsonNode definitionForPolymorphicType(final Class<?> clazz)
      throws JsonMappingException {
    final JsonNode generated = generate(clazz);
    return new ObjectNode(JsonNodeFactory.instance).set(
        "oneOf", generated.get("oneOf")
    );
  }

  private static void rewritePropertiesWithPolymorphicAsDefinition(final ObjectNode properties) {
    final Iterator<String> names = properties.fieldNames();
    while (names.hasNext()) {
      final String name = names.next();
      final JsonNode property = properties.get(name);
      for (final Map.Entry<Class<?>, JsonNode> e : POLYMORPHIC_TYPES.entrySet()) {
        if (property.equals(e.getValue())) {
          properties.set(
              name,
              new ObjectNode(
                  JsonNodeFactory.instance,
                  ImmutableMap.of(
                      "$ref",
                      new TextNode(String.format("#/definitions/%s", e.getKey().getSimpleName())))
              )
          );
        }
      }
    }
  }

  /*
   * by default, JsonSchemaGenerator adds a oneOf type for each property whose
   * type is a polymorphic schema. This method rewrites the schema to include the
   * oneOf schema as a definition, and to reference that definition from each property.
   */
  private static JsonNode rewriteWithPolymorphicAsDefinition(final JsonNode schema) {
    final ObjectNode definitions = (ObjectNode) schema.get("definitions");
    final Iterator<String> keys = definitions.fieldNames();
    while (keys.hasNext()) {
      final ObjectNode type = (ObjectNode) definitions.get(keys.next());
      final ObjectNode properties = (ObjectNode) type.get("properties");
      rewritePropertiesWithPolymorphicAsDefinition(properties);
    }
    for (final Map.Entry<Class<?>, JsonNode> e : POLYMORPHIC_TYPES.entrySet()) {
      definitions.set(e.getKey().getSimpleName(), e.getValue());
    }
    return schema;
  }

  public static JsonNode generate() throws JsonMappingException {
    final JsonSchemaGenerator generator = new JsonSchemaGenerator(MAPPER, configure());
    return rewriteWithPolymorphicAsDefinition(generator.generateJsonSchema(KsqlPlan.class));
  }

  public static void main(final String ...args) throws IOException {
    System.out.print(getJsonText());
  }

  public static void generateTo(final Path schemaPath) throws IOException {
    Files.write(schemaPath, getJsonText().getBytes(Charsets.UTF_8));
  }

  private static String getJsonText() throws JsonProcessingException {
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(generate());
  }
}
