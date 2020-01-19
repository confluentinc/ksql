/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import io.confluent.ksql.api.server.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class BufferedPublisherTest {

  private Vertx vertx;
  private Context context;
  private BufferedPublisher<String> publisher;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    publisher = new BufferedPublisher<>(context);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void shouldWibble() {
    TestSubscriber subscriber = new TestSubscriber();
    publisher.subscribe(subscriber);

  }

  class TestSubscriber implements Subscriber<String> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<String> values = new ArrayList<>();

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      this.sub = sub;
    }

    @Override
    public synchronized void onNext(final String value) {
      values.add(value);
    }

    @Override
    public synchronized void onError(final Throwable t) {
      this.error = t;
    }

    @Override
    public synchronized void onComplete() {
      this.completed = true;
    }

    public boolean isCompleted() {
      return completed;
    }

    public Throwable getError() {
      return error;
    }

    public List<String> getValues() {
      return values;
    }
  }

}
