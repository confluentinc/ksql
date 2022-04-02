/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.api.server.JsonStreamedRowResponseWriter.MAX_FLUSH_MS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonStreamedRowResponseWriterTest {

  private static final String QUERY_ID = "queryId";
  private static final List<String> COL_NAMES = ImmutableList.of(
      "A", "B", "C"
  );
  private static final List<String> COL_TYPES = ImmutableList.of(
      "INTEGER", "DOUBLE", "ARRAY"
  );
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("A"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("B"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("C"), SqlTypes.array(SqlTypes.STRING))
      .build();

  private static final long TIME_NOW_MS = 1000;

  @Mock
  private HttpServerResponse response;
  @Mock
  private QueryPublisher publisher;
  @Mock
  private Clock clock;
  @Mock
  private Context context;
  @Mock
  private Vertx vertx;

  private AtomicLong timeMs = new AtomicLong(TIME_NOW_MS);
  private Runnable simulatedVertxTimerCallback;

  private JsonStreamedRowResponseWriter writer;
  private StringBuilder stringBuilder = new StringBuilder();

  public JsonStreamedRowResponseWriterTest() {
  }

  @Before
  public void setUp() {
    when(response.write((Buffer) any())).thenAnswer(a -> {
      final Buffer buffer = a.getArgument(0);
      stringBuilder.append(new String(buffer.getBytes(), StandardCharsets.UTF_8));
      return Future.succeededFuture();
    });
    when(response.write((String) any())).thenAnswer(a -> {
      final String str = a.getArgument(0);
      stringBuilder.append(str);
      return Future.succeededFuture();
    });
    when(context.owner()).thenReturn(vertx);
    when(clock.millis()).thenAnswer(a -> timeMs.get());


    writer = new JsonStreamedRowResponseWriter(response, publisher, Optional.empty(),
        Optional.empty(), clock, true, context);
  }

  private void setupWithMessages(String completionMessage, String limitMessage, boolean buffering) {
    // No buffering for these responses
    writer = new JsonStreamedRowResponseWriter(response, publisher, Optional.of(completionMessage),
        Optional.of(limitMessage), clock, buffering, context);
  }

  private void expectTimer() {
    AtomicLong delay = new AtomicLong(TIME_NOW_MS);
    when(vertx.setTimer(anyLong(), any())).thenAnswer(a -> {
      Handler<Long> handler = a.getArgument(1);
      simulatedVertxTimerCallback = () -> {
        delay.addAndGet(MAX_FLUSH_MS);
        timeMs.set(delay.get());
        handler.handle(1L);
        simulatedVertxTimerCallback = null;
      };
      return 1L;
    });
  }

  @Test
  public void shouldSucceedWithBuffering_noRows() {
    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx,  never()).setTimer(anyLong(), any());
  }

  @Test
  public void shouldSucceedWithBuffering_oneRow() {
    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\"," 
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx, times(1)).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithBuffering_twoRows_timeout() {
    // Given:
    expectTimer();

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    simulatedVertxTimerCallback.run();
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    simulatedVertxTimerCallback.run();
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}}]"));
    verify(response, times(3)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(2)).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithBuffering_twoRows_noTimeout() {
    // Given:
    expectTimer();

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithBuffering_largeBuffer() {
    // Given:
    expectTimer();

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));

    for (int i = 0; i < 4000; i++) {
      writer.writeRow(new KeyValueMetadata<>(
          KeyValue.keyValue(null, GenericRow.genericRow(i, 100.0d + i,
              ImmutableList.of("hello" + i)))));
    }
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString().split("\n").length, is(4001));
    verify(response, times(4)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(4)).setTimer(anyLong(), any());
    verify(vertx, times(4)).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithCompletionMessage_noBuffering() {
    // Given:
    setupWithMessages("complete!", "limit hit!", false);

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}},\n"
            + "{\"finalMessage\":\"complete!\"}]"));
    verify(response, times(5)).write((Buffer) any());
    verify(response, never()).write((String) any());
    verify(vertx, never()).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithCompletionMessage_buffering() {
    // Given:
    setupWithMessages("complete!", "limit hit!", true);
    expectTimer();

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}},\n"
            + "{\"finalMessage\":\"complete!\"}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithLimitMessage_noBuffering() {
    // Given:
    setupWithMessages("complete!", "limit hit!", false);

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));

    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeLimitMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}},\n"
            + "{\"finalMessage\":\"limit hit!\"}]"));
    verify(response, times(5)).write((Buffer) any());
    verify(response, never()).write((String) any());
    verify(vertx, never()).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithLimitMessage_buffering() {
    // Given:
    setupWithMessages("complete!", "limit hit!", true);
    expectTimer();

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeLimitMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"row\":{\"columns\":[456,789.0,[\"bye\"]]}},\n"
            + "{\"finalMessage\":\"limit hit!\"}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }
}
