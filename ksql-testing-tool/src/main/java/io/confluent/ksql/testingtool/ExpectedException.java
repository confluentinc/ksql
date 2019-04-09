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

package io.confluent.ksql.testingtool;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;

@SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
public class ExpectedException {
  final List<Matcher<?>> matchers = new ArrayList<>();

  public static ExpectedException none() {
    return new ExpectedException();
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
    matchers.add(ThrowableMessageMatcher.hasMessage(matcher));
  }

  @SuppressWarnings("unchecked")
  Matcher<Throwable> build() {
    return allOf(new ArrayList(matchers));
  }
}