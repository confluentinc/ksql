/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import org.apache.kafka.streams.kstream.Grouped;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class GroupedFactoryTest {
  final String opName = "kdot";
  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private StreamsStatics streamsStatics;
  @Mock
  private Grouped<String, GenericRow> grouped;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldCreateGroupedCorrectlyWhenOptimizationsDisabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION)
    );
    when(streamsStatics.groupedWith(null, keySerde, rowSerde)).thenReturn(grouped);

    // When:
    final Grouped returned = GroupedFactory.create(ksqlConfig, streamsStatics).create(
        opName,
        keySerde,
        rowSerde
    );

    // Then:
    assertThat(returned, is(grouped));
    verify(streamsStatics).groupedWith(null, keySerde, rowSerde);
  }

  @Test
  public void shouldCreateGroupedCorrectlyWhenOptimationsEnabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)
    );
    when(streamsStatics.groupedWith(opName, keySerde, rowSerde)).thenReturn(grouped);

    // When:
    final Grouped returned = GroupedFactory.create(ksqlConfig, streamsStatics).create(
        opName,
        keySerde,
        rowSerde
    );

    // Then:
    assertThat(returned, is(grouped));
    verify(streamsStatics).groupedWith(opName, keySerde, rowSerde);
  }
}