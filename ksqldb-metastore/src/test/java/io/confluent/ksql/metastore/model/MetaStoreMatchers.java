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
import io.confluent.ksql.serde.SerdeOption;
import java.util.Optional;
import java.util.Set;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class MetaStoreMatchers {

  private MetaStoreMatchers() {
  }

  public static Matcher<DataSource> hasSerdeOptions(
      final Matcher<? super Iterable<? super SerdeOption>> expected
  ) {
    return new FeatureMatcher<DataSource, Set<SerdeOption>>(
        expected,
        "source with serde options",
        "serde options") {
      @Override
      protected Set<SerdeOption> featureValueOf(final DataSource actual) {
        return actual.getSerdeOptions().all();
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

  public static final class OptionalMatchers {

    private OptionalMatchers() {
    }

    public static <T> Matcher<Optional<T>> of(final Matcher<T> valueMatcher) {
      return new TypeSafeDiagnosingMatcher<Optional<T>>() {
        @Override
        protected boolean matchesSafely(
            final Optional<T> item,
            final Description mismatchDescription
        ) {
          if (!item.isPresent()) {
            mismatchDescription.appendText("not present");
            return false;
          }

          if (!valueMatcher.matches(item.get())) {
            valueMatcher.describeMismatch(item.get(), mismatchDescription);
            return false;
          }

          return true;
        }

        @Override
        public void describeTo(final Description description) {
          description.appendText("optional ").appendDescriptionOf(valueMatcher);
        }
      };
    }
  }
}