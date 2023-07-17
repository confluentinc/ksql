/*
 * Copyright 2023 Confluent Inc.
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

import io.confluent.ksql.util.KsqlStatementException;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;

public class KsqlStatementExceptionMessageMatcher<T extends KsqlStatementException>
    extends ThrowableMessageMatcher<T> {
  private final Matcher<String> matcher;

  public KsqlStatementExceptionMessageMatcher(final Matcher<String> matcher) {
    super(matcher);
    this.matcher = matcher;
  }

  @Override
  protected boolean matchesSafely(final T item) {
    return this.matcher.matches(item.getRawUnloggedDetails())
        || this.matcher.matches(item.getUnloggedMessage());
  }

  @Override
  protected void describeMismatchSafely(final T item, final Description description) {
    description.appendText("unloggedmessage ");
    this.matcher.describeMismatch(item.getRawUnloggedDetails(), description);
  }

  @Factory
  public static <T extends Throwable> Matcher<T> hasMessage(final Matcher<String> matcher) {
    return new KsqlStatementExceptionMessageMatcher(matcher);
  }
}
