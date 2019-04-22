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

package io.confluent.ksql.test.commons;

import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsInstanceOf;

@JsonDeserialize(using = SourceNodeDeserializer.class)
class SourceNode {

  private final String name;
  private final Optional<Class<? extends StructuredDataSource>> type;
  private final Optional<FieldNode> keyField;

  SourceNode(
      @JsonProperty("name") final String name,
      @JsonProperty("type") final String type,
      @JsonProperty("keyField") final Optional<FieldNode> keyField
  ) {
    this.name = name == null ? "" : name;
    this.keyField = keyField;
    this.type = Optional.ofNullable(type)
        .map(String::toUpperCase)
        .map(SourceNode::toType);

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "empty or missing");
    }
  }

  @SuppressWarnings("unchecked")
  Matcher<? super StructuredDataSource<?>> build() {
    if (name.isEmpty()) {
      throw new InvalidFieldException("name", "missing or empty");
    }

    final Matcher<StructuredDataSource<?>> nameMatcher = StructuredDataSourceMatchers
        .hasName(name);

    final Matcher<Object> typeMatcher = type
        .map(IsInstanceOf::instanceOf)
        .orElse(null);

    final Matcher<StructuredDataSource<?>> keyFieldMatcher = keyField
        .map(FieldNode::build)
        .map(StructuredDataSourceMatchers::hasKeyField)
        .orElse(null);

    final Matcher[] matchers = Stream.of(nameMatcher, typeMatcher, keyFieldMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }

  private static Class<? extends StructuredDataSource> toType(final String type) {
    switch (type) {
      case "STREAM":
        return KsqlStream.class;

      case "TABLE":
        return KsqlTable.class;

      default:
        throw new InvalidFieldException("type", "must be either STREAM or TABLE");
    }
  }
}