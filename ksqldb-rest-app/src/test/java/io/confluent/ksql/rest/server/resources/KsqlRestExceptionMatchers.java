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

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public final class KsqlRestExceptionMatchers {

  private KsqlRestExceptionMatchers() {
  }

  /**
   * Matches a {@code KsqlRestException} where the status code matchers the supplier matcher.
   *
   * @param expected matcher defining the expected value(s).
   * @return the matcher.
   */
  public static Matcher<? super KsqlRestException> exceptionStatusCode(
      final Matcher<HttpStatus.Code> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlRestException>() {
      @Override
      protected boolean matchesSafely(
          final KsqlRestException actual,
          final Description mismatchDescription
      ) {
        final Code actualCode = HttpStatus.getCode(actual.getResponse().getStatus());
        if (!expected.matches(actualCode)) {
          mismatchDescription.appendText("but status code ");
          expected.describeMismatch(actualCode, mismatchDescription);
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("status code ")
            .appendDescriptionOf(expected);
      }
    };
  }

  /**
   * Matches a {@code KsqlRestException} where the response entity holds a {@link KsqlErrorMessage}
   * that matches the supplied matcher.
   *
   * @param expected - see {@link io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers}
   * @return the matcher.
   */
  public static Matcher<? super KsqlRestException> exceptionErrorMessage(
      final Matcher<? super KsqlErrorMessage> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlRestException>() {
      @Override
      protected boolean matchesSafely(
          final KsqlRestException actual,
          final Description mismatchDescription
      ) {
        final Object entity = actual.getResponse().getEntity();
        if (!(entity instanceof KsqlErrorMessage)) {
          mismatchDescription
              .appendText("but entity was instance of ")
              .appendValue(entity.getClass());
          return false;
        }

        final KsqlErrorMessage errorMessage = (KsqlErrorMessage) entity;
        if (!expected.matches(errorMessage)) {
          expected.describeMismatch(errorMessage, mismatchDescription);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText(KsqlErrorMessage.class.getSimpleName() + " where ")
            .appendDescriptionOf(expected);
      }
    };
  }

  /**
   * Matches a {@code KsqlRestException} where the response entity holds a
   * {@link KsqlStatementErrorMessage} that matches the supplied matcher.
   *
   * @param expected - see {@link io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers}
   * @return the matcher.
   */
  public static Matcher<? super KsqlRestException> exceptionStatementErrorMessage(
      final Matcher<? super KsqlStatementErrorMessage> expected
  ) {
    return new TypeSafeDiagnosingMatcher<KsqlRestException>() {
      @Override
      protected boolean matchesSafely(
          final KsqlRestException actual,
          final Description mismatchDescription
      ) {
        final Object entity = actual.getResponse().getEntity();
        if (!(entity instanceof KsqlStatementErrorMessage)) {
          mismatchDescription
              .appendText("but entity was instance of ")
              .appendValue(entity.getClass());
          return false;
        }

        final KsqlStatementErrorMessage errorMessage = (KsqlStatementErrorMessage) entity;
        if (!expected.matches(errorMessage)) {
          expected.describeMismatch(errorMessage, mismatchDescription);
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText(KsqlStatementErrorMessage.class.getSimpleName() + " where ")
            .appendDescriptionOf(expected);
      }
    };
  }
}