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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.SchemaParser;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.model.matchers.MetaStoreMatchers;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.JsonParsingUtil;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;

@JsonDeserialize(using = SourceNode.Deserializer.class)
public final class SourceNode {

  private final String name;
  private final String type;
  private final Optional<String> schema;
  private final Optional<KeyFormatNode> keyFormat;
  private final Optional<Set<SerdeOption>> serdeOptions;

  public SourceNode(
      final String name,
      final String type,
      final Optional<String> schema,
      final Optional<KeyFormatNode> keyFormat,
      final Optional<Set<SerdeOption>> serdeOptions
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.serdeOptions = Objects.requireNonNull(serdeOptions, "serdeOptions");

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "missing or empty");
    }

    // Fail early:
    build();
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Optional<KeyFormatNode> getKeyFormat() {
    return keyFormat;
  }

  public Optional<String> getSchema() {
    return schema;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Optional<Set<SerdeOption>> getSerdeOptions() {
    return serdeOptions;
  }

  @SuppressWarnings("unchecked")
  Matcher<? super DataSource> build() {
    if (name.isEmpty()) {
      throw new InvalidFieldException("name", "missing or empty");
    }

    final Matcher<DataSource> nameMatcher = MetaStoreMatchers
        .hasName(name);

    final Matcher<Object> typeMatcher = IsInstanceOf
        .instanceOf(toType(type));

    final Matcher<DataSource> schemaMatcher = schema
        .map(SourceNode::parseSchema)
        .map(Matchers::is)
        .map(MetaStoreMatchers::hasSchema)
        .orElse(null);

    final Matcher<DataSource> keyFormatMatcher = keyFormat
        .map(KeyFormatNode::build)
        .map(MetaStoreMatchers::hasKeyFormat)
        .orElse(null);

    final Matcher<DataSource> serdeOptionsMatcher = serdeOptions
        .map(options -> Matchers.containsInAnyOrder(options.toArray()))
        .map(MetaStoreMatchers::hasSerdeOptions)
        .orElse(null);

    final Matcher<DataSource>[] matchers = Stream
        .of(nameMatcher, typeMatcher, schemaMatcher, keyFormatMatcher, serdeOptionsMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SourceNode that = (SourceNode) o;
    return name.equals(that.name)
        && type.equals(that.type)
        && schema.equals(that.schema)
        && keyFormat.equals(that.keyFormat)
        && serdeOptions.equals(that.serdeOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, schema, keyFormat, serdeOptions);
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

  private static LogicalSchema parseSchema(final String text) {
    return SchemaParser.parse(text, TypeRegistry.EMPTY)
        .toLogicalSchema();
  }

  public static class Deserializer extends JsonDeserializer<SourceNode> {

    @Override
    public SourceNode deserialize(
        final JsonParser jp,
        final DeserializationContext ctxt
    ) throws IOException {
      final JsonNode node = jp.getCodec().readTree(jp);

      final String name = JsonParsingUtil.getRequired("name", node, jp, String.class);
      final String type = JsonParsingUtil.getRequired("type", node, jp, String.class);

      final Optional<String> rawSchema = JsonParsingUtil
          .getOptional("schema", node, jp, String.class);

      final Optional<KeyFormatNode> keyFormat = JsonParsingUtil
          .getOptional("keyFormat", node, jp, KeyFormatNode.class);

      final Optional<Set<SerdeOption>> serdeOptions = JsonParsingUtil
          .getOptional("serdeOptions", node, jp, new TypeReference<Set<SerdeOption>>() { });

      return new SourceNode(name, type, rawSchema, keyFormat, serdeOptions);
    }
  }

  public static SourceNode fromDataSource(final DataSource dataSource) {
    return new SourceNode(
        dataSource.getName().text(),
        dataSource.getDataSourceType().getKsqlType(),
        Optional.of(dataSource.getSchema().toString()),
        Optional.of(KeyFormatNode.fromKeyFormat(dataSource.getKsqlTopic().getKeyFormat())),
        Optional.of(dataSource.getSerdeOptions())
    );
  }
}