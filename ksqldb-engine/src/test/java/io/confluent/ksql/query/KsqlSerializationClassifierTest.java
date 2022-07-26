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

import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.serde.KsqlSerializationException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSerializationClassifierTest {

  @Test
  public void shouldClassifyWrappedKsqlSerializationExceptionWithUserTopicAsUserError() {
    // Given:
    final String topic = "foo.bar";
    final Exception e = new StreamsException(
        new KsqlSerializationException(
            topic,
            "Error serializing message to topic: " + topic,
            new DataException("Struct schemas do not match.")));

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyWrappedKsqlSerializationExceptionWithRepartitionTopicAsUnknownError() {
    // Given:
    final String topic = "_confluent-ksql-default_query_CTAS_USERS_0-Aggregate-GroupBy-repartition";
    final Exception e = new StreamsException(
        new KsqlSerializationException(
            topic,
            "Error serializing message to topic: " + topic,
            new DataException("Struct schemas do not match.")));

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyWrappedKsqlSerializationExceptionWithChangelogTopicAsUnknownError() {
    // Given:
    final String topic = "_confluent-ksql-default_query_CTAS_USERS_0-Aggregate-Aggregate-Materialize-changelog";
    final Exception e = new StreamsException(
        new KsqlSerializationException(
            topic,
            "Error serializing message to topic: " + topic,
            new DataException("Struct schemas do not match.")));

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyKsqlSerializationExceptionWithUserTopicAsUserError() {
    // Given:
    final String topic = "foo.bar";
    final Exception e = new KsqlSerializationException(
        topic,
        "Error serializing message to topic: " + topic);

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyKsqlSerializationExceptionWithRepartitionTopicAsUnknownError() {
    // Given:
    final String topic = "_confluent-ksql-default_query_CTAS_USERS_0-Aggregate-GroupBy-repartition";
    final Exception e = new KsqlSerializationException(
        topic,
        "Error serializing message to topic: " + topic);

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

  @Test
  public void shouldClassifyNonKsqlSerializationExceptionAsUnknownError() {
    // Given:
    final Exception e = new StreamsException(new DataException("Something went wrong"));

    // When:
    final Type type = new KsqlSerializationClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

}