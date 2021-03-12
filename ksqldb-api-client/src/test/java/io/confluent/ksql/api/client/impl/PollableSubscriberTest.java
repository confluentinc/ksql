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

package io.confluent.ksql.api.client.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;

public class PollableSubscriberTest {
  private static Duration POLL_DURATION = Duration.ofMillis(100);
  private static String COLUMN_NAME = "id";

  private Publisher<Row> publisher;

  private Vertx vertx;
  private Throwable throwable;
  private Context context;
  private PollableSubscriber pollableSubscriber;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    pollableSubscriber = new PollableSubscriber(context, t -> throwable = t);
  }

  @Test
  public void shouldPollSingleBatch() {
    shouldPollRows(PollableSubscriber.REQUEST_BATCH_SIZE - 10);
  }

  @Test
  public void shouldPollMultiBatch() {
    shouldPollRows(PollableSubscriber.REQUEST_BATCH_SIZE + 10);
  }

  @Test
  public void shouldSetError() {
    publisher = new BufferedPublisher<Row>(context, ImmutableList.of()) {
      @Override
      protected void maybeSend() {
        sendError(new RuntimeException("Error!"));
      }
    };

    publisher.subscribe(pollableSubscriber);

    Row row = pollableSubscriber.poll(POLL_DURATION);
    assertThat(row, is(nullValue()));
    assertThat(throwable, is(notNullValue()));
    assertThat(throwable.getMessage(), is("Error!"));
  }

  private void shouldPollRows(int numRows) {
    final List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(createRow(i));
    }

    publisher = new BufferedPublisher<>(context, rows);

    publisher.subscribe(pollableSubscriber);

    Row row = pollableSubscriber.poll(POLL_DURATION);
    int i = 0;
    for (; row != null; i++) {
      Long col1 = row.getLong(COLUMN_NAME);
      assertThat(col1, is((long) i));
      row = pollableSubscriber.poll(POLL_DURATION);
    }
    assertThat(i, is(numRows));
    assertThat(throwable, is(nullValue()));
  }

  private Row createRow(long id) {
    return new RowImpl(
        ImmutableList.of(COLUMN_NAME),
        ImmutableList.of(new ColumnTypeImpl("BIGINT")),
        new JsonArray().add(id),
        ImmutableMap.of(COLUMN_NAME, 1));
  }
}
