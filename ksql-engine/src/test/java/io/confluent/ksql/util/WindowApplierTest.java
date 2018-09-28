/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.same;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;


public class WindowApplierTest {

  private KGroupedStream groupedStreamMock;
  private UdafAggregator aggregator;
  private Initializer initializer;

  @Before
  public void init() {
    groupedStreamMock = EasyMock.createNiceMock(KGroupedStream.class);
    aggregator = EasyMock.createNiceMock(UdafAggregator.class);
    initializer = () -> 0;
  }


  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateTumblingWindowAggregate() {
    final TimeWindowedKStream windowedKStream = EasyMock.createNiceMock(TimeWindowedKStream.class);
    final TumblingWindowExpression windowExpression = new TumblingWindowExpression(10, TimeUnit.SECONDS);
    final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> store = Materialized.as("store");

    EasyMock.expect(groupedStreamMock.windowedBy(TimeWindows.of(10000L))).andReturn(windowedKStream);
    EasyMock.expect(windowedKStream.aggregate(same(initializer), same(aggregator), same(store))).andReturn(null);
    EasyMock.replay(groupedStreamMock, windowedKStream);

    final KTable kTable = WindowApplier.applyWindow(
        windowExpression,
        groupedStreamMock,
        initializer,
        aggregator,
        store
    );
    EasyMock.verify(groupedStreamMock, windowedKStream);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateHoppingWindowAggregate() {
    final TimeWindowedKStream windowedKStream = EasyMock.createNiceMock(TimeWindowedKStream.class);
    final HoppingWindowExpression windowExpression = new HoppingWindowExpression(10, TimeUnit.SECONDS, 4, TimeUnit.MILLISECONDS);
    final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> store = Materialized.as("store");

    EasyMock.expect(groupedStreamMock.windowedBy(TimeWindows.of(10000L).advanceBy(4L))).andReturn(windowedKStream);
    EasyMock.expect(windowedKStream.aggregate(same(initializer), same(aggregator), same(store))).andReturn(null);
    EasyMock.replay(groupedStreamMock, windowedKStream);

    WindowApplier.applyWindow( windowExpression, groupedStreamMock, initializer, aggregator, store);
    EasyMock.verify(groupedStreamMock, windowedKStream);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateSessionWindowedStreamWithInactiviyGap() {
    final SessionWindowedKStream sessionWindowed = EasyMock.createNiceMock(SessionWindowedKStream.class);
    final Capture<SessionWindows> sessionWindows = EasyMock.newCapture();
    final SessionWindowExpression windowExpression = new SessionWindowExpression(5, TimeUnit.SECONDS);
    final Materialized<String, GenericRow, SessionStore<Bytes, byte[]>> materialized = Materialized.as("store");

    EasyMock.expect(groupedStreamMock.windowedBy(EasyMock.capture(sessionWindows))).andReturn(sessionWindowed);
    EasyMock.expect(sessionWindowed.aggregate(same(initializer),
        same(aggregator),
        anyObject(Merger.class),
        same(materialized))).andReturn(null);
    EasyMock.replay(groupedStreamMock, aggregator, sessionWindowed);

    WindowApplier.applyWindow(
        windowExpression,
        groupedStreamMock,
        initializer,
        aggregator,
        materialized);

    assertThat(sessionWindows.getValue().inactivityGap(), equalTo(5000L));
    EasyMock.verify(groupedStreamMock, aggregator, sessionWindowed);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetMergerForSessionWindowsFromUdafAggregator() {
    final SessionWindowedKStream sessionWindowed = EasyMock.createNiceMock(SessionWindowedKStream.class);
    final Capture<SessionWindows> sessionWindows = EasyMock.newCapture();
    final SessionWindowExpression windowExpression = new SessionWindowExpression(5, TimeUnit.SECONDS);
    final Materialized<String, GenericRow, SessionStore<Bytes, byte[]>> materialized = Materialized.as("store");
    final Merger<String, GenericRow> merger = (s, genericRow, v1) -> genericRow;

    EasyMock.expect(groupedStreamMock.windowedBy(EasyMock.capture(sessionWindows))).andReturn(sessionWindowed);
    EasyMock.expect(sessionWindowed.aggregate(same(initializer),
        same(aggregator),
        same(merger),
        same(materialized))).andReturn(null);
    EasyMock.expect(aggregator.getMerger()).andReturn(merger);
    EasyMock.replay(groupedStreamMock, aggregator, sessionWindowed);

    WindowApplier.applyWindow(windowExpression, groupedStreamMock, initializer, aggregator, materialized);

    EasyMock.verify(aggregator);

  }


}
