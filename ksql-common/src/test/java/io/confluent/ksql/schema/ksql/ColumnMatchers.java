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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Optional;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class ColumnMatchers {

  private ColumnMatchers() {
  }

  public static Matcher<Column> column(
      final SourceName source,
      final ColumnName name
  ) {
    return allOf(
        hasSource(Optional.of(source)),
        hasName(name)
    );
  }

  public static Matcher<Column> column(
      final SourceName source,
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.of(source)),
        hasName(name),
        hasType(type)
    );
  }

  public static Matcher<Column> metaColumn(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.empty()),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.META)
    );
  }

  public static Matcher<Column> metaColumn(
      final SourceName source,
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.of(source)),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.META)
    );
  }

  public static Matcher<Column> keyColumn(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.empty()),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.KEY)
    );
  }

  public static Matcher<Column> keyColumn(
      final SourceName source,
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.of(source)),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.KEY)
    );
  }

  public static Matcher<Column> valueColumn(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.empty()),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.VALUE)
    );
  }

  public static Matcher<Column> valueColumn(
      final SourceName source,
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(Optional.of(source)),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.VALUE)
    );
  }

  public static Matcher<Column> valueColumn(
      final Optional<SourceName> source,
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasSource(source),
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.VALUE)
    );
  }

  public static Matcher<Column> hasSource(final Optional<SourceName> source) {
    return new FeatureMatcher<Column, Optional<SourceName>>(
        is(source),
        "column with source",
        "source"
    ) {
      @Override
      protected Optional<SourceName> featureValueOf(final Column actual) {
        return actual.source();
      }
    };
  }

  public static Matcher<Column> hasName(final ColumnName name) {
    return new FeatureMatcher<Column, ColumnName>(
        is(name),
        "column with name",
        "name"
    ) {
      @Override
      protected ColumnName featureValueOf(final Column actual) {
        return actual.name();
      }
    };
  }

  public static Matcher<Column> hasType(final SqlType type) {
    return new FeatureMatcher<Column, SqlType>(
        is(type),
        "column with type",
        "type"
    ) {
      @Override
      protected SqlType featureValueOf(final Column actual) {
        return actual.type();
      }
    };
  }

  private static Matcher<Column> hasNamespace(final Namespace ns) {
    return new FeatureMatcher<Column, Namespace>(
        is(ns),
        "column with type",
        "type"
    ) {
      @Override
      protected Namespace featureValueOf(final Column actual) {
        return actual.namespace();
      }
    };
  }
}