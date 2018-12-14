/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class KsqlErrorMessageMatchers {

  private KsqlErrorMessageMatchers() {
  }

  /**
   * Matches a {@code KsqlErrorMessage} where the error message matches the supplied matcher.
   *
   * @param expected - the message matcher
   * @return the matcher.
   */
  public static Matcher<? super KsqlErrorMessage> errorMessage(
      final Matcher<? extends String> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlErrorMessage>() {
      @Override
      protected boolean matchesSafely(
          final KsqlErrorMessage actual,
          final Description mismatchDescription
      ) {
        if (!expected.matches(actual.getMessage())) {
          mismatchDescription.appendText("but message ");
          expected.describeMismatch(actual.getMessage(), mismatchDescription);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("message ")
            .appendDescriptionOf(expected);
      }
    };
  }

  /**
   * Matches a {@code KsqlErrorMessage} where the error code matches the supplied matcher.
   *
   * @param expected - the message matcher
   * @return the matcher.
   */
  public static Matcher<? super KsqlErrorMessage> errorCode(
      final Matcher<Integer> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlErrorMessage>() {
      @Override
      protected boolean matchesSafely(
          final KsqlErrorMessage actual,
          final Description mismatchDescription
      ) {
        if (!expected.matches(actual.getErrorCode())) {
          mismatchDescription.appendText("but error code ");
          expected.describeMismatch(actual.getErrorCode(), mismatchDescription);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("error code ")
            .appendDescriptionOf(expected);
      }
    };
  }
}