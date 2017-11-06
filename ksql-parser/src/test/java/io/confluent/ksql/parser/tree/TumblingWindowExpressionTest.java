/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;

import static org.easymock.EasyMock.same;

public class TumblingWindowExpressionTest {

  @Test
  public void shouldCreateTumblingWindowAggregate() {
    final KGroupedStream stream = EasyMock.createNiceMock(KGroupedStream.class);
    final TimeWindowedKStream windowedKStream = EasyMock.createNiceMock(TimeWindowedKStream.class);
    final UdafAggregator aggregator = EasyMock.createNiceMock(UdafAggregator.class);
    final TumblingWindowExpression windowExpression = new TumblingWindowExpression(10, TimeUnit.SECONDS);
    final Initializer initializer = () -> 0;
    final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> store = Materialized.as("store");

    EasyMock.expect(stream.windowedBy(TimeWindows.of(10000L))).andReturn(windowedKStream);
    EasyMock.expect(windowedKStream.aggregate(same(initializer), same(aggregator), same(store))).andReturn(null);
    EasyMock.replay(stream, windowedKStream);

    windowExpression.applyAggregate(stream, initializer, aggregator, store);
    EasyMock.verify(stream, windowedKStream);
  }

}