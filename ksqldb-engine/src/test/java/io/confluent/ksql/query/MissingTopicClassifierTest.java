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
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.services.KafkaTopicClient;
import java.util.Set;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MissingTopicClassifierTest {

  @Test
  public void shouldClassifyMissingTopicAsUserError() {
    // Given:
    final Exception e = new MissingSourceTopicException("foo");

    // When:
    final Type type = new MissingTopicClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.USER));
  }

  @Test
  public void shouldClassifyNoMissingTopicAsUnknownError() {
    // Given:
    final Exception e = new Exception("foo");

    // When:
    final Type type = new MissingTopicClassifier("").classify(e);

    // Then:
    assertThat(type, is(Type.UNKNOWN));
  }

}