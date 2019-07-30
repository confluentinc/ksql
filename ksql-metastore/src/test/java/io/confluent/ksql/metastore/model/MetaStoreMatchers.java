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

import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class MetaStoreMatchers {

  private MetaStoreMatchers() {
  }

  public static Matcher<DataSource<?>> hasName(final String name) {
    return new FeatureMatcher<DataSource<?>, String>
        (is(name), "source with name", "name") {
      @Override
      protected String featureValueOf(final DataSource<?> actual) {
        return actual.getName();
      }
    };
  }

  public static Matcher<DataSource<?>> hasKeyField(
      final Matcher<KeyField> fieldMatcher
  ) {
    return new FeatureMatcher<DataSource<?>, KeyField>
        (fieldMatcher, "source with key field", "key field") {
      @Override
      protected KeyField featureValueOf(final DataSource<?> actual) {
        return actual.getKeyField();
      }
    };
  }

  public static Matcher<DataSource<?>> hasValueSchema(
      final Matcher<Schema> schemaMatcher
  ) {
    return new FeatureMatcher<DataSource<?>, Schema>
        (schemaMatcher, "source with value schema", "value schema") {
      @Override
      protected Schema featureValueOf(final DataSource<?> actual) {
        return actual.getSchema().valueSchema();
      }
    };
  }

  public static Matcher<DataSource<?>> hasSerdeOptions(
      final Matcher<Iterable<? super SerdeOption>> expected
  ) {
    return new FeatureMatcher<DataSource<?>, Set<SerdeOption>>(
        expected,
        "source with serde options",
        "serde options") {
      @Override
      protected Set<SerdeOption> featureValueOf(final DataSource<?> actual) {
        return actual.getSerdeOptions();
      }
    };
  }

  public static Matcher<DataSource<?>> hasKeyFormat(
      final Matcher<? super KeyFormat> matcher
  ) {
    return new FeatureMatcher<DataSource<?>, KeyFormat>(
        matcher,
        "source with key format",
        "key format") {
      @Override
      protected KeyFormat featureValueOf(final DataSource<?> actual) {
        return actual.getKsqlTopic().getKeyFormat();
      }
    };
  }

  public static final class KeyFieldMatchers {

    private KeyFieldMatchers() {
    }

    public static Matcher<KeyField> hasName(final String name) {
      return hasName(Optional.of(name));
    }

    public static Matcher<KeyField> hasName(final Optional<String> name) {
      return new FeatureMatcher<KeyField, Optional<String>>
          (is(name), "field with name", "name") {
        @Override
        protected Optional<String> featureValueOf(final KeyField actual) {
          return actual.name();
        }
      };
    }

    public static Matcher<KeyField> hasLegacyName(final String name) {
      return hasLegacyName(Optional.of(name));
    }

    public static Matcher<KeyField> hasLegacyName(final Optional<String> name) {
      return new FeatureMatcher<KeyField, Optional<String>>
          (is(name), "field with legacy name", "legacy name") {
        @Override
        protected Optional<String> featureValueOf(final KeyField actual) {
          return actual.legacy().map(LegacyField::name);
        }
      };
    }

    public static Matcher<KeyField> hasLegacyType(final SqlType schema) {
      return hasLegacyType(Optional.of(schema));
    }

    public static Matcher<KeyField> hasLegacyType(final Optional<? extends SqlType> schema) {
      return new FeatureMatcher<KeyField, Optional<SqlType>>
          (is(schema), "field with legacy type", "legacy type") {
        @Override
        protected Optional<SqlType> featureValueOf(final KeyField actual) {
          return actual.legacy().map(LegacyField::type);
        }
      };
    }
  }

  public static final class LegacyFieldMatchers {

    private LegacyFieldMatchers() {
    }

    public static Matcher<LegacyField> hasName(final String name) {
      return new FeatureMatcher<LegacyField, String>
          (is(name), "field with name", "name") {
        @Override
        protected String featureValueOf(final LegacyField actual) {
          return actual.name();
        }
      };
    }

    public static Matcher<LegacyField> hasType(final SqlType type) {
      return new FeatureMatcher<LegacyField, SqlType>
          (is(type), "field with type", "type") {
        @Override
        protected SqlType featureValueOf(final LegacyField actual) {
          return actual.type();
        }
      };
    }
  }

  public static final class FieldMatchers {

    private FieldMatchers() {
    }

    public static Matcher<Field> hasFullName(final String name) {
      return new FeatureMatcher<Field, String>
          (is(name), "field with name", "name") {
        @Override
        protected String featureValueOf(final Field actual) {
          return actual.fullName();
        }
      };
    }

    public static Matcher<Field> hasType(final SqlType type) {
      return new FeatureMatcher<Field, SqlType>
          (is(type), "field with type", "type") {
        @Override
        protected SqlType featureValueOf(final Field actual) {
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