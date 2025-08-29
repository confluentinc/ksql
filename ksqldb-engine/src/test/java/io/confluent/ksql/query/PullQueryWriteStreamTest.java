/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.pull.StreamedRowTranslator;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryWriteStreamTest {

  private static final QueryId TEST_ID = new QueryId("test");
  private static final int TEST_TIMEOUT_SECONDS = 60;
  private static final int LIMIT = 5;

  @Rule
  public final Timeout timeout = Timeout.seconds(TEST_TIMEOUT_SECONDS);

  @Mock private LogicalSchema schema;
  private PullQueryWriteStream writeStream;
  private ScheduledExecutorService executorService;

  @Before
  public void setUp() {
    final StreamedRowTranslator translator = new StreamedRowTranslator(schema, Optional.empty());
    writeStream = new PullQueryWriteStream(OptionalInt.of(LIMIT), translator);
    writeStream.write(ImmutableList.of(StreamedRow.header(TEST_ID, schema)));

    executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @After
  public void tearDown() {
    executorService.shutdown();
  }

  @Test
  public void shouldAcceptWrites() {
    // When:
    writeStream.write(getData(1));

    // Then:
    assertThat(writeStream.getTotalRowsQueued(), is(1));
    assertThat(writeStream.size(), is(1));
  }

  @Test
  public void shouldPoll() {
    // Given:
    final AtomicBoolean written = new AtomicBoolean(false);
    writeStream.write(getData(1), ignored -> written.set(true));

    // When:
    final KeyValueMetadata<List<?>, GenericRow> row = writeStream.poll();

    // Then:
    assertThat(row.getKeyValue().value().get(0), is(0));
    assertThat("expected written callback to be called", written.get());
    assertThat("expected writeStream to be empty after poll", writeStream.isEmpty());
  }

  @Test
  public void shouldHandleLimit() {
    // Given:
    final AtomicBoolean limit = new AtomicBoolean(false);
    final AtomicBoolean end = new AtomicBoolean(false);

    writeStream.setLimitHandler(() -> limit.set(true));
    writeStream.setCompletionHandler(() -> end.set(true));

    // When:
    writeStream.write(getData(LIMIT + 1));

    // Then:
    assertThat("expected limit handler to be called", limit.get());
    assertThat("expected end handler to be called", end.get());
    assertThat("expected queue to be done", writeStream.isDone());
    assertThat(writeStream.size(), is(LIMIT)); // should not enqueue more than LIMIT
  }

  @Test
  public void shouldCallDrainHandlerWhenHasCapacity() {
    // Given:
    final Context context = mock(Context.class);
    @SuppressWarnings("unchecked") final Handler<Void> handler = mock(Handler.class);

    writeStream.setWriteQueueMaxSize(1);
    writeStream.write(getData(1));
    try (MockedStatic<Vertx> mocked = mockStatic(Vertx.class)) {
      mocked.when(Vertx::currentContext).thenReturn(context);
      writeStream.drainHandler(handler);
    }

    // When:
    writeStream.poll();

    // Then:
    verify(context).runOnContext(handler);
  }

  @Test
  public void shouldAwaitCapacity() throws InterruptedException {
    // Given:
    writeStream.setWriteQueueMaxSize(1);
    writeStream.write(getData(1));

    // When:
    executorService.schedule(() -> writeStream.poll(), 100, TimeUnit.MILLISECONDS);
    writeStream.awaitCapacity(TEST_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

    // Then:
    // does not time out
  }

  @Test
  public void shouldAwaitData() throws InterruptedException {
    // Given:
    writeStream.setWriteQueueMaxSize(1);

    // When:
    executorService.schedule(() -> writeStream.write(getData(1)), 100, TimeUnit.MILLISECONDS);
    writeStream.poll(TEST_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

    // Then:
    // does not time out
  }

  private List<StreamedRow> getData(final int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> StreamedRow.pullRow(GenericRow.genericRow(i), Optional.empty()))
        .collect(Collectors.toList());
  }

}