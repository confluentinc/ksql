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

package io.confluent.ksql.api.client.util;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public final class ClientTestUtil {

  private ClientTestUtil() {
  }

  public static void shouldReceiveRows(
      final Publisher<Row> publisher,
      final int numRows,
      final Consumer<List<Row>> rowsVerifier,
      final boolean subscriberCompleted
  ) {
    TestSubscriber<Row> subscriber = new TestSubscriber<Row>() {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(numRows);
      }
    };
    publisher.subscribe(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(numRows));

    rowsVerifier.accept(subscriber.getValues());

    assertThatEventually(subscriber::isCompleted, equalTo(subscriberCompleted));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  public static <T> TestSubscriber<T> subscribeAndWait(final Publisher<T> publisher) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    TestSubscriber<T> subscriber = new TestSubscriber<T>() {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        latch.countDown();
      }
    };
    publisher.subscribe(subscriber);
    awaitLatch(latch);
    return subscriber;
  }

  public static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
  }

  public static int compareRowByOrderedLong(final Row row1, final Row row2) {
    return Long.compare(row1.getLong("LONG"), row2.getLong("LONG"));
  }

  public static int compareKsqlArrayByOrderedLong(final KsqlArray row1, final KsqlArray row2) {
    return Long.compare(row1.getLong(2), row2.getLong(2));
  }

  public static class TestSubscriber<T> implements Subscriber<T> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<T> values = new ArrayList<>();

    public TestSubscriber() {
    }

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      this.sub = sub;
    }

    @Override
    public synchronized void onNext(final T value) {
      values.add(value);
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public synchronized void onError(final Throwable t) {
      this.error = t;
    }

    @Override
    public synchronized void onComplete() {
      this.completed = true;
    }

    public synchronized boolean isCompleted() {
      return completed;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public synchronized Throwable getError() {
      return error;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public synchronized List<T> getValues() {
      return values;
    }

    public synchronized Subscription getSub() {
      return sub;
    }
  }
}