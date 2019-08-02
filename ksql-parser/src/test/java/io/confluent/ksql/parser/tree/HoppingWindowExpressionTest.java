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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
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
public class HoppingWindowExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

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
  private HoppingWindowExpression windowExpression;

  @Before
  public void setUp() {
    windowExpression = new HoppingWindowExpression(10, SECONDS, 4, TimeUnit.MILLISECONDS);

    when(stream
        .windowedBy(any(TimeWindows.class)))
        .thenReturn(windowedKStream);
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new HoppingWindowExpression(10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(Optional.of(SOME_LOCATION), 10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(Optional.of(OTHER_LOCATION), 10, SECONDS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(30, SECONDS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, HOURS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, SECONDS, 1, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, SECONDS, 20, MILLISECONDS)
        )
        .testEquals();
  }

  @Test
  public void shouldCreateHoppingWindowAggregate() {
    // When:
    windowExpression.applyAggregate(stream, initializer, aggregator, store);

    // Then:
    verify(stream)
        .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofMillis(4L)));

    verify(windowedKStream).aggregate(initializer, aggregator, store);
  }

  @Test
  public void shouldReturnWindowInfo() {
    assertThat(new HoppingWindowExpression(10, SECONDS, 20, MINUTES).getWindowInfo(),
        is(WindowInfo.of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10)))));
  }
}