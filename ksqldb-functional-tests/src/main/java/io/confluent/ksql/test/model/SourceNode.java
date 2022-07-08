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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
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
import io.confluent.ksql.serde.SerdeFeature;
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
  private final Optional<String> valueFormat;
  private final Optional<Set<SerdeFeature>> keyFeatures;
  private final Optional<Set<SerdeFeature>> valueFeatures;
  private final Optional<Boolean> isSource;

  public SourceNode(
      final String name,
      final String type,
      final Optional<String> schema,
      final Optional<KeyFormatNode> keyFormat,
      final Optional<String> valueFormat,
      final Optional<Set<SerdeFeature>> keyFeatures,
      final Optional<Set<SerdeFeature>> valueFeatures,
      final Optional<Boolean> isSource
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.keyFeatures = Objects.requireNonNull(keyFeatures, "keyFeatures");
    this.valueFeatures = Objects.requireNonNull(valueFeatures, "valueFeatures");
    this.isSource = Objects.requireNonNull(isSource, "isSource");

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

  public Optional<String> getValueFormat() {
    return valueFormat;
  }

  public Optional<String> getSchema() {
    return schema;
  }

  @JsonInclude(Include.NON_EMPTY)
  public Optional<Set<SerdeFeature>> getKeyFeatures() {
    return keyFeatures;
  }

  @JsonInclude(Include.NON_EMPTY)
  public Optional<Set<SerdeFeature>> getValueFeatures() {
    return valueFeatures;
  }

  public Optional<Boolean> getIsSource() {
    return isSource;
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

    final Matcher<DataSource> keyFmtMatcher = keyFormat
        .map(KeyFormatNode::build)
        .map(MetaStoreMatchers::hasKeyFormat)
        .orElse(null);

    final Matcher<DataSource> valueFmtMatcher = valueFormat
        .map(Matchers::is)
        .map(MetaStoreMatchers::hasValueFormat)
        .orElse(null);

    final Matcher<DataSource> keyFeatsMatcher = keyFeatures
        .map(features -> Matchers.containsInAnyOrder(features.toArray()))
        .map(MetaStoreMatchers::hasKeySerdeFeatures)
        .orElse(null);

    final Matcher<DataSource> valFeatsMatcher = valueFeatures
        .map(features -> Matchers.containsInAnyOrder(features.toArray()))
        .map(MetaStoreMatchers::hasValueSerdeFeatures)
        .orElse(null);

    final Matcher<DataSource> isSourceMatcher = isSource
        .map(Matchers::is)
        .map(MetaStoreMatchers::isSourceMatches)
        .orElse(null);

    final Matcher<DataSource>[] matchers = Stream.of(
        nameMatcher,
        typeMatcher,
        schemaMatcher,
        keyFmtMatcher,
        valueFmtMatcher,
        keyFeatsMatcher,
        valFeatsMatcher,
        isSourceMatcher
    ).filter(Objects::nonNull).toArray(Matcher[]::new);

    return allOf(matchers);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
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
        && keyFeatures.equals(that.keyFeatures)
        && valueFormat.equals(that.valueFormat)
        && valueFeatures.equals(that.valueFeatures)
        && isSource.equals(that.isSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, schema, keyFormat, valueFormat,
        keyFeatures, valueFeatures, isSource);
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

      final Optional<String> valueFormat = JsonParsingUtil
          .getOptional("valueFormat", node, jp, String.class);

      final Optional<Set<SerdeFeature>> keyFeatures = JsonParsingUtil
          .getOptional("keyFeatures", node, jp, new TypeReference<Set<SerdeFeature>>() {
          });

      final Optional<Set<SerdeFeature>> valueFeatures = JsonParsingUtil
          .getOptional("valueFeatures", node, jp, new TypeReference<Set<SerdeFeature>>() {
          });
      final Optional<Boolean> isSource = JsonParsingUtil
          .getOptional("isSource", node, jp, Boolean.class);

      return new SourceNode(
          name,
          type,
          rawSchema,
          keyFormat,
          valueFormat,
          keyFeatures,
          valueFeatures,
          isSource
      );
    }
  }

  public static SourceNode fromDataSource(final DataSource dataSource) {
    return new SourceNode(
        dataSource.getName().text(),
        dataSource.getDataSourceType().getKsqlType(),
        Optional.of(dataSource.getSchema().toString()),
        Optional.of(KeyFormatNode.fromKeyFormat(dataSource.getKsqlTopic().getKeyFormat())),
        Optional.of(dataSource.getKsqlTopic().getValueFormat().getFormatInfo().getFormat()),
        Optional.of(dataSource.getKsqlTopic().getKeyFormat().getFeatures().all()),
        Optional.of(dataSource.getKsqlTopic().getValueFormat().getFeatures().all()),
        Optional.of(dataSource.isSource())
    );
  }
}