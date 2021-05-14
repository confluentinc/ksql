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
import io.confluent.ksql.execution.streams.JoinedFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinedFactoryTest {

  private static final String OP_NAME = "kdot";

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

  @Test
  public void shouldCreateJoinedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    when(joiner.joinedWith(keySerde, leftSerde, rightSerde, OP_NAME)).thenReturn(joined);

    // When:
    final Joined<String, GenericRow, GenericRow> returned
        = JoinedFactory.create(joiner).create(
        keySerde, leftSerde, rightSerde, OP_NAME);

    // Then:
    assertThat(returned, is(joined));
    verify(joiner).joinedWith(keySerde, leftSerde, rightSerde, OP_NAME);
  }
}