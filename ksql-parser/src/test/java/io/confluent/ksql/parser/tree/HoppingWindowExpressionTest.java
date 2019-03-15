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
import static org.easymock.EasyMock.same;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.easymock.EasyMock;
import org.junit.Test;

public class HoppingWindowExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

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

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateHoppingWindowAggregate() {
    final KGroupedStream stream = EasyMock.createNiceMock(KGroupedStream.class);
    final TimeWindowedKStream windowedKStream = EasyMock.createNiceMock(TimeWindowedKStream.class);
    final UdafAggregator aggregator = EasyMock.createNiceMock(UdafAggregator.class);
    final HoppingWindowExpression windowExpression = new HoppingWindowExpression(10, SECONDS, 4, TimeUnit.MILLISECONDS);
    final Initializer initializer = () -> 0;
    final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> store = Materialized.as("store");

    EasyMock.expect(stream.windowedBy(TimeWindows.of(Duration.ofMillis(10000L)).advanceBy(Duration.ofMillis(4L)))).andReturn(windowedKStream);
    EasyMock.expect(windowedKStream.aggregate(same(initializer), same(aggregator), same(store))).andReturn(null);
    EasyMock.replay(stream, windowedKStream);

    windowExpression.applyAggregate(stream, initializer, aggregator, store);
    EasyMock.verify(stream, windowedKStream);
  }

}