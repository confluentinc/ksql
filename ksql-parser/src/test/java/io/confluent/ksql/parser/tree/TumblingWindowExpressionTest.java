/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TumblingWindowExpressionTest {

  @Mock
  private KGroupedStream<Struct, GenericRow> stream;
  @Mock
  private TimeWindowedKStream<Struct, GenericRow> windowedKStream;
  @Mock
  private UdafAggregator aggregator;
  @Mock
  private Initializer<GenericRow> initializer;
  @Mock
  private Materialized<Struct, GenericRow, WindowStore<Bytes, byte[]>> store;
  private TumblingWindowExpression windowExpression;

  @Before
  public void setUp() {
    windowExpression = new TumblingWindowExpression(10, TimeUnit.SECONDS);

    when(stream
        .windowedBy(any(TimeWindows.class)))
        .thenReturn(windowedKStream);
  }

  @Test
  public void shouldCreateTumblingWindowAggregate() {
    // When:
    windowExpression.applyAggregate(stream, initializer, aggregator, store);

    // Then:
    verify(stream).windowedBy(TimeWindows.of(Duration.ofSeconds(10)));
    verify(windowedKStream).aggregate(initializer, aggregator, store);
  }

  @Test
  public void shouldReturnWindowInfo() {
    assertThat(new TumblingWindowExpression(11, SECONDS).getWindowInfo(),
        is(WindowInfo.of(WindowType.TUMBLING, Optional.of(Duration.ofSeconds(11)))));
  }
}