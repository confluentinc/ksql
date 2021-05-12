/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.query.QueryError.Type;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthorizationClassifierTest {

  @Test
  public void shouldClassifyWrappedAuthorizationExceptionAsUserError() {
    // Given:
    final Exception e = new StreamsException(new AuthorizationException("foo"));

    // When:
    final Type type = new AuthorizationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyAuthorizationExceptionAsUserError() {
    // Given:
    final Exception e = new AuthorizationException("foo");

    // When:
    final Type type = new AuthorizationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifySubTypeOfAuthorizationExceptionAsUserError() {
    // Given:
    final Exception e = new TopicAuthorizationException("foo");

    // When:
    final Type type = new AuthorizationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyNoTransactionalIdAuthorizationExceptionAsUnknownError() {
    // Given:
    final Exception e = new Exception("foo");

    // When:
    final Type type = new AuthorizationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

}