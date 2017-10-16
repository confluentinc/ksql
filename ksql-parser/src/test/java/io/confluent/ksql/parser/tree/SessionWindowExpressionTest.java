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
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.same;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SessionWindowExpressionTest {

  private final KGroupedStream groupedStreamMock = EasyMock.createNiceMock(KGroupedStream.class);
  private final SessionWindowedKStream sessionWindowed = EasyMock.createNiceMock(SessionWindowedKStream.class);
  private final UdafAggregator aggregator = EasyMock.createNiceMock(UdafAggregator.class);
  private final SessionWindowExpression expression = new SessionWindowExpression(5, TimeUnit.SECONDS);
  private final Initializer initializer = () -> 0;
  private final Materialized<String, GenericRow, SessionStore<Bytes, byte[]>> materialized = Materialized.as("store");
  private final Capture<SessionWindows> sessionWindows = EasyMock.newCapture();
  private final Merger<String, GenericRow> merger = (s, genericRow, v1) -> genericRow;

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateSessionWindowedStreamWithInactiviyGap() {
    EasyMock.expect(groupedStreamMock.windowedBy(EasyMock.capture(sessionWindows))).andReturn(sessionWindowed);
    EasyMock.expect(sessionWindowed.aggregate(same(initializer),
        same(aggregator),
        anyObject(Merger.class),
        same(materialized))).andReturn(null);
    EasyMock.replay(groupedStreamMock, aggregator, sessionWindowed);

    expression.applyAggregate(groupedStreamMock, initializer, aggregator, materialized);

    assertThat(sessionWindows.getValue().inactivityGap(), equalTo(5000L));
    EasyMock.verify(groupedStreamMock);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetMergerForSessionWindowsFromUdafAggregator() {
    EasyMock.expect(groupedStreamMock.windowedBy(EasyMock.capture(sessionWindows))).andReturn(sessionWindowed);
    EasyMock.expect(sessionWindowed.aggregate(same(initializer),
        same(aggregator),
        same(merger),
        same(materialized))).andReturn(null);
    EasyMock.expect(aggregator.getMerger()).andReturn(merger);
    EasyMock.replay(groupedStreamMock, aggregator, sessionWindowed);

    expression.applyAggregate(groupedStreamMock, initializer, aggregator, materialized);

    EasyMock.verify(aggregator);

  }

}