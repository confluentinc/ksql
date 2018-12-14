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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class MaterializedFactoryTest {
  final String opName = "kdot";
  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private MaterializedFactory.Materializer materializer;
  @Mock
  private Materialized<String, GenericRow, StateStore> materialized;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void shouldCreateMaterializedCorrectlyWhenOptimizationsDisabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
            StreamsConfig.NO_OPTIMIZATION,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
    );
    when(materializer.materializedWith(keySerde, rowSerde)).thenReturn(materialized);

    // When:
    final Materialized<String, GenericRow, StateStore> returned
        = MaterializedFactory.create(ksqlConfig, materializer).create(
            keySerde, rowSerde, opName);

    // Then:
    assertThat(returned, is(materialized));
    verify(materializer).materializedWith(keySerde, rowSerde);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCreateJoinedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        ImmutableMap.of(
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
    );
    final Materialized asName = mock(Materialized.class);
    when(materializer.materializedAs(opName)).thenReturn(asName);
    final Materialized withKeySerde = mock(Materialized.class);
    when(asName.withKeySerde(keySerde)).thenReturn(withKeySerde);
    final Materialized withRowSerde = mock(Materialized.class);
    when(withKeySerde.withValueSerde(rowSerde)).thenReturn(withRowSerde);

    // When:
    final Materialized<String, GenericRow, StateStore> returned
        = MaterializedFactory.create(ksqlConfig, materializer).create(
            keySerde, rowSerde, opName);

    // Then:
    assertThat(returned, is(withRowSerde));
    verify(materializer).materializedAs(opName);
    verify(asName).withKeySerde(keySerde);
    verify(withKeySerde).withValueSerde(rowSerde);
  }
}
