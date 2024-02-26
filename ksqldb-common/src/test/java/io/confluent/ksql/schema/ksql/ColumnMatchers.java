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
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Optional;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class ColumnMatchers {

  private ColumnMatchers() {
  }

  public static <T extends SimpleColumn> Matcher<T> column(final ColumnName name) {
    return hasName(name);
  }

  public static <T extends SimpleColumn> Matcher<T> column(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasName(name),
        hasType(type)
    );
  }

  public static <T extends Column> Matcher<T> keyColumn(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.KEY)
    );
  }

  public static <T extends Column> Matcher<T> valueColumn(
      final ColumnName name,
      final SqlType type
  ) {
    return allOf(
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.VALUE)
    );
  }

  public static <T extends Column> Matcher<T> headersColumn(
      final ColumnName name,
      final SqlType type,
      final Optional<String> headerKey
  ) {
    return allOf(
        hasName(name),
        hasType(type),
        hasNamespace(Namespace.HEADERS),
        hasHeaderKey(headerKey)
    );
  }

  public static <T extends SimpleColumn> Matcher<T> hasName(final ColumnName name) {
    return new FeatureMatcher<T, ColumnName>(
        is(name),
        "column with name",
        "name"
    ) {
      @Override
      protected ColumnName featureValueOf(final SimpleColumn actual) {
        return actual.name();
      }
    };
  }

  public static <T extends SimpleColumn> Matcher<T> hasType(final SqlType type) {
    return new FeatureMatcher<T, SqlType>(
        is(type),
        "column with type",
        "type"
    ) {
      @Override
      protected SqlType featureValueOf(final SimpleColumn actual) {
        return actual.type();
      }
    };
  }

  private static <T extends Column> Matcher<T> hasNamespace(final Namespace ns) {
    return new FeatureMatcher<T, Namespace>(
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

  private static <T extends Column> Matcher<T> hasHeaderKey(final Optional<String> key) {
    return new FeatureMatcher<T, Optional<String>>(
        is(key),
        "column with header key",
        "header key"
    ) {
      @Override
      protected Optional<String> featureValueOf(final Column actual) {
        return actual.headerKey();
      }
    };
  }
}