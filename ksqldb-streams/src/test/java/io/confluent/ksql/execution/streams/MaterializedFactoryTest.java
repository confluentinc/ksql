/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaterializedFactoryTest {

  private static final String OP_NAME = "kdot";

  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private MaterializedFactory.Materializer materializer;
  @Mock
  private Optional<Duration> retention;

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldCreateJoinedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    final Materialized asName = mock(Materialized.class);
    when(materializer.materializedAs(OP_NAME, Optional.empty())).thenReturn(asName);
    final Materialized withKeySerde = mock(Materialized.class);
    when(asName.withKeySerde(keySerde)).thenReturn(withKeySerde);
    final Materialized withRowSerde = mock(Materialized.class);
    when(withKeySerde.withValueSerde(rowSerde)).thenReturn(withRowSerde);

    // When:
    final Materialized<String, GenericRow, StateStore> returned
        = MaterializedFactory.create(materializer).create(
        keySerde, rowSerde, OP_NAME);

    // Then:
    assertThat(returned, is(withRowSerde));
    verify(materializer).materializedAs(OP_NAME, Optional.empty());
    verify(asName).withKeySerde(keySerde);
    verify(withKeySerde).withValueSerde(rowSerde);
  }

  @Test
  public void shouldSetupRetentionWhenNonEmpty() {
    // Given:
    when(retention.isPresent()).thenReturn(true);
    when(retention.get()).thenReturn(Duration.ofSeconds(10));

    // When:
    MaterializedFactory.create().create(keySerde, rowSerde, OP_NAME, retention);

    // Then:
    verify(retention).get();
  }
}
