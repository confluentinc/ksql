/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Joined;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class JoinedFactoryTest {
  final String opName = "kdot";
  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;
  @Mock
  private Serde<GenericRow> rightSerde;
  @Mock
  private JoinedFactory.Joiner joiner;
  @Mock
  private Joined<String, GenericRow, GenericRow> joined;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldCreateJoinedCorrectlyWhenOptimizationsDisabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
            StreamsConfig.NO_OPTIMIZATION,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
    );
    when(joiner.joinedWith(keySerde, leftSerde, rightSerde, null)).thenReturn(joined);

    // When:
    final Joined<String, GenericRow, GenericRow> returned
        = JoinedFactory.create(ksqlConfig, joiner).create(
            keySerde, leftSerde, rightSerde, opName);

    // Then:
    assertThat(returned, is(joined));
    verify(joiner).joinedWith(keySerde, leftSerde, rightSerde, null);
  }

  @Test
  public void shouldCreateJoinedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
    );
    when(joiner.joinedWith(keySerde, leftSerde, rightSerde, opName)).thenReturn(joined);

    // When:
    final Joined<String, GenericRow, GenericRow> returned
        = JoinedFactory.create(ksqlConfig, joiner).create(
        keySerde, leftSerde, rightSerde, opName);

    // Then:
    assertThat(returned, is(joined));
    verify(joiner).joinedWith(keySerde, leftSerde, rightSerde, opName);
  }
}