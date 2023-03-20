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
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;

@SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
public class KsqlExpectedException {
  public final List<Matcher<?>> matchers = new ArrayList<>();

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
    matchers.add(new KsqlExceptionMatcher<>(ThrowableMessageMatcher.hasMessage(matcher),
        KsqlStatementExceptionMessageMatcher.hasMessage(matcher)));
  }

  @SuppressWarnings("unchecked")
  public Matcher<Throwable> build() {
    return allOf((List)new ArrayList<>(matchers));
  }

  private static class KsqlExceptionMatcher<T extends Throwable> extends TypeSafeMatcher<T> {
    private final Matcher<T> matcher1;
    private final Matcher<T> matcher2;

    KsqlExceptionMatcher(final Matcher<T> matcher1, final Matcher<T> matcher2) {
      this.matcher1 = matcher1;
      this.matcher2 = matcher2;
    }

    public void describeTo(final Description description) {
      description.appendText("CompoundMatchers:");
      description.appendDescriptionOf(this.matcher1);
      description.appendDescriptionOf(this.matcher2);
    }

    protected boolean matchesSafely(final T item) {
      return this.matcher1.matches(item) || this.matcher2.matches(item);
    }

    protected void describeMismatchSafely(final T item, final Description description) {
      description.appendText("message ");
      this.matcher1.describeMismatch(item.getMessage(), description);
      this.matcher2.describeMismatch(item.getMessage(), description);

    }
  }
}
