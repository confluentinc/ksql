/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class KsqlExceptionMatcher {
  private KsqlExceptionMatcher() {
  }

  public static Matcher<?  super KsqlStatementException> statementText(
      final Matcher<String> statementMatcher
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlStatementException>() {
      @Override
      protected boolean matchesSafely(
          final KsqlStatementException actual,
          final Description mismatchDescription) {
        if (!statementMatcher.matches(actual.getSqlStatement())) {
          statementMatcher.describeMismatch(actual.getSqlStatement(), mismatchDescription);
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("statement text ").appendDescriptionOf(statementMatcher);
      }
    };
  }

  public static Matcher<?  super KsqlStatementException> rawMessage(
      final Matcher<String> messageMatcher
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlStatementException>() {
      @Override
      protected boolean matchesSafely(
          final KsqlStatementException actual,
          final Description mismatchDescription) {
        if (!messageMatcher.matches(actual.getRawMessage())) {
          messageMatcher.describeMismatch(actual.getRawMessage(), mismatchDescription);
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("raw message ").appendDescriptionOf(messageMatcher);
      }
    };
  }
}