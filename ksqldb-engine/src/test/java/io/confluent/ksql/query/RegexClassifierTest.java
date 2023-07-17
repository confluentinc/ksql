/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.query.QueryError.Type;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RegexClassifierTest {

  @Mock
  private Throwable error;
  @Mock
  private Throwable cause;

  @Test
  public void shouldClassifyWithRegex() {
    // Given:
    final QueryErrorClassifier classifier = RegexClassifier.fromConfig("USER .*foo.*", "id");
    givenMessage(error, "foo");

    // When:
    final Type type = classifier.classify(error);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyNPEIfConfigured() {
    // Given:
    final QueryErrorClassifier classifier =
        RegexClassifier.fromConfig("USER .*NullPointerException", "id");

    // When:
    final Type type = classifier.classify(npe());

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyNPEAsUnknwon() {
    // Given:
    final QueryErrorClassifier classifier = RegexClassifier.fromConfig("USER .*foo.*", "id");

    // When:
    final Type type = classifier.classify(npe());

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyWithRegexInCause() {
    // Given:
    final QueryErrorClassifier classifier = RegexClassifier.fromConfig("USER .*foo.*", "id");
    when(error.getCause()).thenReturn(cause);
    givenMessage(error, "bar");
    givenMessage(cause, "foo");

    // When:
    final Type type = classifier.classify(error);

    // Then:
    assertThat(type, is(Type.USER));
  }


  @Test
  public void shouldClassifyAsUnknownIfBadRegex() {
    // Given:
    final QueryErrorClassifier classifier = RegexClassifier.fromConfig("USER", "id");
    givenMessage(error, "foo");

    // When:
    final Type type = classifier.classify(error);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyAsUnknown() {
    // Given:
    final QueryErrorClassifier classifier = RegexClassifier.fromConfig("USER .*foo.*", "id");
    when(error.getCause()).thenReturn(cause);
    givenMessage(error, "bar");
    givenMessage(cause, "baz");

    // When:
    final Type type = classifier.classify(error);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  private void givenMessage(final Throwable error, final String message) {
    when(error.getMessage()).thenReturn(message);
  }

  private Exception npe() {
    try {
      return throwNpe(null);
    } catch (final Exception e) {
      return e;
    }
  }

  private Exception throwNpe(final String arg) {
    return new Exception(arg.concat("foo"));
  }
}