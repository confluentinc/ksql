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

package io.confluent.ksql.test.model.matchers;

import static org.hamcrest.Matchers.is;

import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.ColumnReferenceParser;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import java.util.Optional;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class MetaStoreMatchers {

  private MetaStoreMatchers() {
  }

  public static Matcher<DataSource> hasName(final String name) {
    return new FeatureMatcher<DataSource, SourceName>(
        is(SourceName.of(name)),
        "source with name",
        "name"
    ) {
      @Override
      protected SourceName featureValueOf(final DataSource actual) {
        return actual.getName();
      }
    };
  }

  public static Matcher<DataSource> hasKeyField(
      final Matcher<KeyField> fieldMatcher
  ) {
    return new FeatureMatcher<DataSource, KeyField>(
        fieldMatcher,
        "source with key field",
        "key field"
    ) {
      @Override
      protected KeyField featureValueOf(final DataSource actual) {
        return actual.getKeyField();
      }
    };
  }

  public static Matcher<DataSource> hasSchema(
      final Matcher<LogicalSchema> schemaMatcher
  ) {
    return new FeatureMatcher<DataSource, LogicalSchema>(
        schemaMatcher,
        "source with schema",
        "schema"
    ) {
      @Override
      protected LogicalSchema featureValueOf(final DataSource actual) {
        return actual.getSchema();
      }
    };
  }

  public static Matcher<DataSource> hasKeyFormat(
      final Matcher<? super KeyFormat> matcher
  ) {
    return new FeatureMatcher<DataSource, KeyFormat>(
        matcher,
        "source with key format",
        "key format") {
      @Override
      protected KeyFormat featureValueOf(final DataSource actual) {
        return actual.getKsqlTopic().getKeyFormat();
      }
    };
  }

  public static final class KeyFieldMatchers {

    private KeyFieldMatchers() {
    }

    public static Matcher<KeyField> hasName(final Optional<String> name) {
      return new FeatureMatcher<KeyField, Optional<ColumnName>>(
          is(name.map(ColumnReferenceParser::parse).map(ColumnRef::name)),
          "field with name",
          "name"
      ) {
        @Override
        protected Optional<ColumnName> featureValueOf(final KeyField actual) {
          return actual.ref().map(ColumnRef::name);
        }
      };
    }
  }
}