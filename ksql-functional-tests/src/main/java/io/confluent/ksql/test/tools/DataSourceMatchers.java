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

package io.confluent.ksql.test.tools;

import static org.hamcrest.Matchers.is;

import io.confluent.ksql.metastore.model.DataSource;
import java.util.Optional;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class DataSourceMatchers {

  private DataSourceMatchers() {
  }

  static Matcher<DataSource<?>> hasName(final String name) {
    return new FeatureMatcher<DataSource<?>, String>(
        is(name),
        "source with name",
        "name") {
      @Override
      protected String featureValueOf(final DataSource<?> actual) {
        return actual.getName();
      }
    };
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