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

import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MissingSchemaClassifierTest {
  @Test
  public void shouldClassifyMissingSchemaAsUserError() {
    // Given:
    final Exception e = new RestClientException("foo", 404, 40403);

    // When:
    final QueryError.Type type = new MissingSchemaClassifier("").classify(e);

    // Then:
    assertThat(type, is(QueryError.Type.USER));
  }

  @Test
  public void shouldClassifyOtherExceptionAsUnknownException() {
    // Given:
    final Exception e = new Exception("foo");

    // When:
    final QueryError.Type type = new MissingSchemaClassifier("").classify(e);

    // Then:
    assertThat(type, is(QueryError.Type.UNKNOWN));
  }
}
