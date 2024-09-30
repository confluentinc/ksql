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

package io.confluent.ksql.test.tools.exceptions;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Helper class for testing exceptions.
 */
@SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
public class KsqlExpectedException {
  public final List<Matcher<?>> matchers = new ArrayList<>();
  private Matcher<Throwable> expectedCause;

  public static KsqlExpectedException none() {
    return new KsqlExpectedException();
  }

  public void expect(final Class<? extends Throwable> type) {
    matchers.add(instanceOf(type));
  }

  public void expect(final Matcher<?> matcher) {
    matchers.add(matcher);
  }

  public void expectMessage(final String substring) {
    expectMessage(containsString(substring));
  }

  public void expectMessage(final Matcher<String> matcher) {
    matchers.add(UnloggedMessageMatcher.hasMessage(matcher));
  }

  public void expectCause(final Matcher<Throwable> causeMatcher) {
    this.expectedCause = causeMatcher;
  }

  /**
   * Matcher that matches the message of a {@link KsqlStatementException} or its unlogged message.
   * @param <T> the type of the exception
   */
  public static class UnloggedMessageMatcher<T extends Throwable> extends TypeSafeMatcher<T> {
    private final Matcher<String> matcher;

    public UnloggedMessageMatcher(final Matcher<String> matcher) {
      this.matcher = matcher;
    }

    public void describeTo(final Description description) {
      description.appendText("exception with message or unloggedDetails ");
      description.appendDescriptionOf(this.matcher);
    }

    protected boolean matchesSafely(final T item) {
      final boolean matches = this.matcher.matches(item.getMessage());
      return matches || (
          item instanceof KsqlStatementException
              && this.matcher.matches(((KsqlStatementException) item).getUnloggedMessage())
        );
    }

    protected void describeMismatchSafely(final T item, final Description description) {
      description.appendText("message ");
      this.matcher.describeMismatch(item.getMessage(), description);
      if (item instanceof KsqlStatementException) {
        description.appendText("unloggedDetails ");
        this.matcher.describeMismatch(
            ((KsqlStatementException) item).getUnloggedMessage(),
            description
        );
      }
    }

    @Factory
    public static <T extends Throwable> Matcher<T> hasMessage(final Matcher<String> matcher) {
      return new UnloggedMessageMatcher<>(matcher);
    }
  }

  @SuppressWarnings("unchecked")
  public Matcher<Throwable> build() {
    final List<Matcher<?>> allMatchers = new ArrayList<>(matchers);
    if (expectedCause != null) {
      allMatchers.add(new TypeSafeMatcher<Throwable>() {
        @Override
        protected boolean matchesSafely(final Throwable item) {
          return expectedCause.matches(item.getCause());
        }

        @Override
        public void describeTo(final Description description) {
          description.appendText("exception with cause ");
          expectedCause.describeTo(description);
        }
      });
    }
    return allOf((List) allMatchers);
  }
}
