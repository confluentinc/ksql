/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.test.util;

import com.google.common.base.Objects;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Add some missing map matchers.
 */
public final class MapMatchers {

  private MapMatchers() {
  }

  /**
   * A map size matcher to work around the fact this is missing from hamcrest.
   *
   * @param sizeMatcher the expected size of the map.
   * @param <K> the key of the map.
   * @param <V> the value of the map.
   * @return the matcher.
   */
  public static <K, V> Matcher<Map<? extends K, ? extends V>> mapHasSize(
      final Matcher<? super Integer> sizeMatcher
  ) {
    return new FeatureMatcher<Map<? extends K, ? extends V>, Integer>(
        sizeMatcher, "a map with size", "map size") {

      @Override
      protected Integer featureValueOf(final Map<? extends K, ? extends V> actual) {
        return actual.size();
      }
    };
  }

  public static <K, V> Matcher<? super Map<? super K, ? super V>> mapHasItems(
      final Map<? super K, ? super V> items
  ) {
    return new TypeSafeDiagnosingMatcher<Map<? super K, ? super V>>() {
      @SuppressWarnings("SuspiciousMethodCalls")
      @Override
      protected boolean matchesSafely(
          final Map<? super K, ? super V> actual,
          final Description mismatchDescription) {

        final Set<? super K> missingKeys = new HashSet<>(items.keySet());
        missingKeys.removeAll(actual.keySet());

        if (!missingKeys.isEmpty()) {
          mismatchDescription
              .appendText("did not contain " + missingKeys.size() + " keys ")
              .appendValue(missingKeys);
          return false;
        }

        // Do not switch to streams, as they can't handle nulls:
        final Map<Object, Object> differentValues = new HashMap<>();
        items.forEach((key, value) -> {
          final Object actualValue = actual.get(key);
          if (!Objects.equal(value, actualValue)) {
            differentValues.put(key, actualValue);
          }
        });

        if (!differentValues.isEmpty()) {
          mismatchDescription
              .appendText("has different values for " + differentValues.size() + " keys ")
              .appendValue(differentValues);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("is map containing items ")
            .appendValue(items);
      }
    };
  }
}
