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

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.RemoteEndpoint.Async;
import javax.websocket.RemoteEndpoint.Basic;
import javax.websocket.Session;

import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;

import static org.junit.Assert.assertEquals;

public class WebSocketSubscriberTest {

  @Test
  public void testSanity() throws Exception {
    Session session = EasyMock.mock(Session.class);
    Async async = EasyMock.mock(Async.class);

    WebSocketSubscriber<Map<String, Object>> subscriber = new WebSocketSubscriber<>(session, new ObjectMapper());

    Subscription subscription = EasyMock.mock(Subscription.class);

    subscription.request(1);
    EasyMock.expectLastCall().once();
    EasyMock.replay(subscription);

    subscriber.onSubscribe(subscription);

    EasyMock.verify(subscription);

    EasyMock.reset(subscription, session, async);

    EasyMock.expect(session.getAsyncRemote()).andReturn(async).anyTimes();
    Capture<String> json = EasyMock.newCapture(CaptureType.ALL);
    async.sendText(EasyMock.capture(json), EasyMock.anyObject());
    EasyMock.expectLastCall().times(3);

    subscription.request(1);
    EasyMock.expectLastCall().once();

    session.close(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    subscription.cancel();

    EasyMock.replay(subscription, session, async);
    subscriber.onNext(ImmutableList.of(ImmutableMap.of("a", 1), ImmutableMap.of("b", 2), ImmutableMap.of("c", 3)));
    assertEquals(ImmutableList.of("{\"a\":1}","{\"b\":2}","{\"c\":3}"), json.getValues());
    subscriber.onComplete();
    subscriber.close();

    EasyMock.verify(subscription, session, async);
  }

  @Test
  public void testStopSendingAfterClose() throws Exception {
    Session session = EasyMock.mock(Session.class);
    Async async = EasyMock.mock(Async.class);

    WebSocketSubscriber<Map<String, Object>> subscriber = new WebSocketSubscriber<>(session, new ObjectMapper());
    Subscription subscription = EasyMock.mock(Subscription.class);

    subscription.request(1);
    EasyMock.expectLastCall().once();
    EasyMock.replay(subscription);
    subscriber.onSubscribe(subscription);
    EasyMock.verify(subscription);
    EasyMock.reset(subscription, session, async);

    EasyMock.expect(session.getAsyncRemote()).andReturn(async).anyTimes();
    Capture<String> json = EasyMock.newCapture(CaptureType.ALL);
    async.sendText(EasyMock.capture(json), EasyMock.anyObject());
    subscription.request(1);
    subscription.cancel();

    EasyMock.replay(subscription, session, async);
    subscriber.onNext(ImmutableList.of(ImmutableMap.of("a", 1)));
    subscriber.close();
    subscriber.onNext(ImmutableList.of(ImmutableMap.of("b", 2), ImmutableMap.of("c", 3)));
    assertEquals(ImmutableList.of("{\"a\":1}"), json.getValues());

    EasyMock.verify(subscription, session, async);
  }


  @Test
  public void testOnSchema() throws Exception {
    Session session = EasyMock.mock(Session.class);
    Basic basic = EasyMock.mock(Basic.class);
    ObjectMapper mapper = new ObjectMapper();
    new SchemaMapper().registerToObjectMapper(mapper);

    WebSocketSubscriber<Map<String, Object>> subscriber = new WebSocketSubscriber<>(session, mapper);
    Subscription subscription = EasyMock.mock(Subscription.class);

    subscription.request(1);
    EasyMock.expectLastCall().once();

    EasyMock.replay(subscription);
    subscriber.onSubscribe(subscription);
    EasyMock.verify(subscription);
    EasyMock.reset(subscription);

    session.getBasicRemote();
    EasyMock.expectLastCall().andReturn(basic).once();
    Capture<String> schema = EasyMock.newCapture();
    basic.sendText(EasyMock.capture(schema));
    EasyMock.expectLastCall().andThrow(new IOException("bad bad io")).once();

    Capture<CloseReason> reason = EasyMock.newCapture();
    session.close(EasyMock.capture(reason));
    subscription.cancel();

    EasyMock.replay(subscription, session, basic);

    subscriber.onSchema(SchemaBuilder.struct().build());
    subscriber.close();

    assertEquals("{\"type\":\"struct\",\"fields\":[],\"optional\":false}", schema.getValue());
    assertEquals("Unable to send schema", reason.getValue().getReasonPhrase());
    assertEquals(CloseCodes.PROTOCOL_ERROR, reason.getValue().getCloseCode());

    EasyMock.verify(subscription, session, basic);
  }

  @Test
  public void testOnError() throws Exception {
    Session session = EasyMock.mock(Session.class);
    ObjectMapper mapper = new ObjectMapper();
    new SchemaMapper().registerToObjectMapper(mapper);

    WebSocketSubscriber<Map<String, Object>> subscriber = new WebSocketSubscriber<>(session, mapper);
    Subscription subscription = EasyMock.mock(Subscription.class);

    subscription.request(1);
    EasyMock.expectLastCall().once();

    EasyMock.replay(subscription);
    subscriber.onSubscribe(subscription);
    EasyMock.verify(subscription);
    EasyMock.reset(subscription);

    Capture<CloseReason> reason = EasyMock.newCapture();
    EasyMock.expect(session.getId()).andReturn("abc123").once();
    session.close(EasyMock.capture(reason));
    EasyMock.expectLastCall().once();
    subscription.cancel();
    EasyMock.expectLastCall().once();

    EasyMock.replay(subscription, session);

    subscriber.onError(new RuntimeException("streams died"));
    subscriber.close();

    assertEquals("streams exception", reason.getValue().getReasonPhrase());
    assertEquals(CloseCodes.UNEXPECTED_CONDITION, reason.getValue().getCloseCode());

    EasyMock.verify(subscription, session);
  }
}
