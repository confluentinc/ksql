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

package io.confluent.ksql.rest.entity;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class KsqlStatementErrorMessageMatchers {

  private KsqlStatementErrorMessageMatchers() {
  }

  /**
   * Matches a {@code KsqlStatementErrorMessage} for a specific statement.
   *
   * @param expected - the statement matcher
   * @return the matcher.
   */
  public static Matcher<? super KsqlStatementErrorMessage> statement(
      final Matcher<? extends String> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlStatementErrorMessage>() {
      @Override
      protected boolean matchesSafely(
          final KsqlStatementErrorMessage actual,
          final Description mismatchDescription
      ) {
        if (!expected.matches(actual.getStatementText())) {
          mismatchDescription.appendText("but statement ");
          expected.describeMismatch(actual.getStatementText(), mismatchDescription);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("statement ")
            .appendDescriptionOf(expected);
      }
    };
  }
}