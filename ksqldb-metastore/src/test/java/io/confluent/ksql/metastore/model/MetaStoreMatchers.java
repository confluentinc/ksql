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

package io.confluent.ksql.metastore.model;

import static org.hamcrest.Matchers.is;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.SerdeFeature;
import java.util.Optional;
import java.util.Set;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class MetaStoreMatchers {

  private MetaStoreMatchers() {
  }

  public static Matcher<DataSource> hasValueSerdeFeatures(
      final Matcher<? super Iterable<? super SerdeFeature>> expected
  ) {
    return new FeatureMatcher<DataSource, Set<SerdeFeature>>(
        expected,
        "source with value serde features",
        "value serde features") {
      @Override
      protected Set<SerdeFeature> featureValueOf(final DataSource actual) {
        return actual.getKsqlTopic().getValueFormat().getFeatures().all();
      }
    };
  }

  public static final class FieldMatchers {

    private FieldMatchers() {
    }

    public static Matcher<Column> hasFullName(final String name) {
      return new FeatureMatcher<Column, String>
          (is(name), "field with name", "name") {
        @Override
        protected String featureValueOf(final Column actual) {
          return actual.name().text();
        }
      };
    }

    public static Matcher<Column> hasFullName(final ColumnName name) {
      return new FeatureMatcher<Column, String>
          (is(name), "field with name", "name") {
        @Override
        protected String featureValueOf(final Column actual) {
          return actual.name().text();
        }
      };
    }

    public static Matcher<Column> hasType(final SqlType type) {
      return new FeatureMatcher<Column, SqlType>
          (is(type), "field with type", "type") {
        @Override
        protected SqlType featureValueOf(final Column actual) {
          return actual.type();
        }
      };
    }
  }


}