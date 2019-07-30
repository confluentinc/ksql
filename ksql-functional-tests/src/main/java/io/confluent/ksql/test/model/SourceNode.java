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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.MetaStoreMatchers;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.TypeContextUtil;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
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
class SourceNode {

  private final String name;
  private final Optional<Class<? extends DataSource>> type;
  private final Optional<KeyFieldNode> keyField;
  private final Optional<Schema> valueSchema;
  private final Optional<KeyFormatNode> keyFormat;

  SourceNode(
      @JsonProperty(value = "name", required = true) final String name,
      @JsonProperty(value = "type", required = true) final String type,
      @JsonProperty("keyField") final KeyFieldNode keyField,
      @JsonProperty("valueSchema") final String valueSchema,
      @JsonProperty("keyFormat") final KeyFormatNode keyFormat
  ) {
    this.name = name == null ? "" : name;
    this.keyField = Optional.ofNullable(keyField);
    this.valueSchema = parseSchema(valueSchema);
    this.keyFormat = Optional.ofNullable(keyFormat);
    this.type = Optional.ofNullable(type)
        .map(String::toUpperCase)
        .map(SourceNode::toType);

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

    final Matcher<Object> typeMatcher = type
        .map(IsInstanceOf::instanceOf)
        .orElse(null);

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
    switch (type) {
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
        .map(TypeContextUtil::getType)
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
}