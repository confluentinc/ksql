/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.query.QueryError.Type;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlFunctionClassifierTest {

  @Test
  public void shouldClassifyWrappedKsqlFunctionExceptionAsUserError() {
    // Given:
    final Exception e = new StreamsException(new KsqlFunctionException("foo"));

    // When:
    final Type type = new KsqlFunctionClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyKsqlFunctionExceptionAsUserError() {
    // Given:
    final Exception e = new KsqlFunctionException("foo");

    // When:
    final Type type = new KsqlFunctionClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyWrappedStreamsExceptionWithoutKsqlFunctionExceptionAsUnknownError() {
    // Given:
    final Exception e = new StreamsException(new ArithmeticException());

    // When:
    final Type type = new KsqlFunctionClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyStreamsExceptionAsUnknownError() {
    // Given:
    final Exception e = new StreamsException("foo");

    // When:
    final Type type = new KsqlFunctionClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyGenericExceptionAsUnknownError() {
    // Given:
    final Exception e = new Exception("foo");

    // When:
    final Type type = new KsqlFunctionClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

}