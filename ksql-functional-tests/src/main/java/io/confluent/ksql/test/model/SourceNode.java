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

package io.confluent.ksql.test.model;

import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.test.model.matchers.MetaStoreMatchers;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.JsonParsingUtil;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;

@SuppressWarnings("rawtypes")
@JsonDeserialize(using = SourceNode.Deserializer.class)
final class SourceNode {

  private final String name;
  private final Class<? extends DataSource> type;
  private final Optional<KeyFieldNode> keyField;
  private final Optional<Schema> valueSchema;
  private final Optional<KeyFormatNode> keyFormat;

  private SourceNode(
      final String name,
      final Class<? extends DataSource> type,
      final Optional<KeyFieldNode> keyField,
      final Optional<Schema> valueSchema,
      final Optional<KeyFormatNode> keyFormat
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.valueSchema = Objects.requireNonNull(valueSchema, "valueSchema");
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "missing or empty");
    }
  }

  @SuppressWarnings("unchecked")
  Matcher<? super DataSource<?>> build() {
    if (name.isEmpty()) {
      throw new InvalidFieldException("name", "missing or empty");
    }

    final Matcher<DataSource<?>> nameMatcher = MetaStoreMatchers
        .hasName(name);

    final Matcher<Object> typeMatcher = IsInstanceOf
        .instanceOf(type);

    final Matcher<DataSource<?>> keyFieldMatcher = keyField
        .map(KeyFieldNode::build)
        .map(MetaStoreMatchers::hasKeyField)
        .orElse(null);

    final Matcher<DataSource<?>> valueSchemaMatcher = valueSchema
        .map(Matchers::is)
        .map(MetaStoreMatchers::hasValueSchema)
        .orElse(null);

    final Matcher<DataSource<?>> keyFormatMatcher = keyFormat
        .map(KeyFormatNode::build)
        .map(MetaStoreMatchers::hasKeyFormat)
        .orElse(null);

    final Matcher<DataSource<?>>[] matchers = Stream
        .of(nameMatcher, typeMatcher, keyFieldMatcher, valueSchemaMatcher, keyFormatMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }

  private static Class<? extends DataSource> toType(final String type) {
    switch (type.toUpperCase()) {
      case "STREAM":
        return KsqlStream.class;

      case "TABLE":
        return KsqlTable.class;

      default:
        throw new InvalidFieldException("type", "must be either STREAM or TABLE");
    }
  }

  private static Optional<Schema> parseSchema(final String schema) {
    return Optional.ofNullable(schema)
        .map(schemaString -> SqlTypeParser.create(TypeRegistry.EMPTY).parse(schemaString))
        .map(Type::getSqlType)
        .map(SchemaConverters.sqlToConnectConverter()::toConnectSchema)
        .map(SourceNode::makeTopLevelStructNoneOptional);
  }

  private static ConnectSchema makeTopLevelStructNoneOptional(final Schema schema) {
    if (schema.type() != Schema.Type.STRUCT) {
      return (ConnectSchema) schema.schema();
    }

    final SchemaBuilder builder = SchemaBuilder.struct();
    schema.fields().forEach(field -> builder.field(field.name(), field.schema()));
    return (ConnectSchema) builder.build();
  }

  public static class Deserializer extends JsonDeserializer<SourceNode> {

    @Override
    public SourceNode deserialize(
        final JsonParser jp,
        final DeserializationContext ctxt
    ) throws IOException {
      final JsonNode node = jp.getCodec().readTree(jp);

      final String name = JsonParsingUtil.getRequired("name", node, jp, String.class);
      final Class<? extends DataSource> type = toType(
          JsonParsingUtil.getRequired("type", node, jp, String.class)
      );

      final Optional<KeyFieldNode> keyField = JsonParsingUtil
          .getOptionalOrElse("keyField", node, jp, KeyFieldNode.class, KeyFieldNode.none());

      final Optional<Schema> valueSchema = JsonParsingUtil
          .getOptional("valueSchema", node, jp, String.class)
          .flatMap(SourceNode::parseSchema);

      final Optional<KeyFormatNode> keyFormat = JsonParsingUtil
          .getOptional("keyFormat", node, jp, KeyFormatNode.class);

      return new SourceNode(name, type, keyField, valueSchema, keyFormat);
    }
  }
}