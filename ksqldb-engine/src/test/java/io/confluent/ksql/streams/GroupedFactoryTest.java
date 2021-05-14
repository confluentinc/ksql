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

package io.confluent.ksql.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.GroupedFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupedFactoryTest {

  private static final String OP_NAME = "kdot";

  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private GroupedFactory.Grouper grouper;
  @Mock
  private Grouped<String, GenericRow> grouped;

  @Test
  public void shouldCreateGroupedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    when(grouper.groupedWith(OP_NAME, keySerde, rowSerde)).thenReturn(grouped);

    // When:
    final Grouped returned = GroupedFactory.create(grouper).create(
        OP_NAME,
        keySerde,
        rowSerde
    );

    // Then:
    assertThat(returned, is(grouped));
    verify(grouper).groupedWith(OP_NAME, keySerde, rowSerde);
  }
}