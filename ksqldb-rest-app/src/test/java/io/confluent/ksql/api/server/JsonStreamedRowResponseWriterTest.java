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
import static org.junit.Assert.assertThrows;
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

  private JsonStreamedRowResponseWriter jsonWriter;
  private JsonStreamedRowResponseWriter protoWriter;
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


    jsonWriter = new JsonStreamedRowResponseWriter(response, publisher, Optional.empty(),
        Optional.empty(), clock, true, context, JsonStreamedRowResponseWriter.RowFormat.JSON);

    protoWriter = new JsonStreamedRowResponseWriter(response, publisher, Optional.empty(),
            Optional.empty(), clock, true, context, JsonStreamedRowResponseWriter.RowFormat.PROTOBUF);
  }

  private void setupWithMessages(String completionMessage, String limitMessage, boolean buffering) {
    // No buffering for these responses
    jsonWriter = new JsonStreamedRowResponseWriter(response, publisher, Optional.of(completionMessage),
        Optional.of(limitMessage), clock, buffering, context, JsonStreamedRowResponseWriter.RowFormat.JSON);

    protoWriter = new JsonStreamedRowResponseWriter(response, publisher, Optional.of(completionMessage),
            Optional.of(limitMessage), clock, buffering, context, JsonStreamedRowResponseWriter.RowFormat.PROTOBUF);
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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    simulatedVertxTimerCallback.run();
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    simulatedVertxTimerCallback.run();
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));

    for (int i = 0; i < 4000; i++) {
      jsonWriter.writeRow(new KeyValueMetadata<>(
          KeyValue.keyValue(null, GenericRow.genericRow(i, 100.0d + i,
              ImmutableList.of("hello" + i)))));
    }
    jsonWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString().split("\n").length, is(4001));
    verify(response, times(38)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(38)).setTimer(anyLong(), any());
    verify(vertx, times(38)).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithCompletionMessage_noBuffering() {
    // Given:
    setupWithMessages("complete!", "limit hit!", false);

    // When:
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    jsonWriter.writeCompletionMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));

    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    jsonWriter.writeLimitMessage().end();

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
    jsonWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    jsonWriter.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    jsonWriter.writeLimitMessage().end();

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

  @Test
  public void shouldSucceedWithBuffering_noRows_proto() {
    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n" +
                    "\\n" +
                    "message ConnectDefault1 {\\n" +
                    "  int32 A = 1;\\n" +
                    "  double B = 2;\\n" +
                    "  repeated string C = 3;\\n" +
                    "}\\n" +
                    "\"}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx,  never()).setTimer(anyLong(), any());
  }

  @Test
  public void shouldSucceedWithBuffering_oneRow_proto() {
    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx, times(1)).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithBuffering_twoRows_timeout_proto() {
    // Given:
    expectTimer();

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    simulatedVertxTimerCallback.run();
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    simulatedVertxTimerCallback.run();
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}}]"));
    verify(response, times(3)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(2)).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithBuffering_twoRows_noTimeout_proto() {
    // Given:
    expectTimer();

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithCompletionMessage_noBuffering_proto() {
    // Given:
    setupWithMessages("complete!", "limit hit!", false);

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}},\n" +
                    "{\"finalMessage\":\"complete!\"}]"));
    verify(response, times(5)).write((Buffer) any());
    verify(response, never()).write((String) any());
    verify(vertx, never()).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithCompletionMessage_buffering_proto() {
    // Given:
    setupWithMessages("complete!", "limit hit!", true);
    expectTimer();

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    protoWriter.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}},\n" +
                    "{\"finalMessage\":\"complete!\"}]"));

    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithLimitMessage_noBuffering_proto() {
    // Given:
    setupWithMessages("complete!", "limit hit!", false);

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));

    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    protoWriter.writeLimitMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}},\n" +
                    "{\"finalMessage\":\"limit hit!\"}]"));
    verify(response, times(5)).write((Buffer) any());
    verify(response, never()).write((String) any());
    verify(vertx, never()).setTimer(anyLong(), any());
    verify(vertx, never()).cancelTimer(anyLong());
  }

  @Test
  public void shouldSucceedWithLimitMessage_buffering_proto() {
    // Given:
    setupWithMessages("complete!", "limit hit!", true);
    expectTimer();

    // When:
    protoWriter.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    protoWriter.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    protoWriter.writeLimitMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                            "\\n" +
                            "message ConnectDefault1 {\\n" +
                            "  int32 A = 1;\\n" +
                            "  double B = 2;\\n" +
                            "  repeated string C = 3;\\n" +
                            "}\\n" +
                            "\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}},\n" +
                    "{\"row\":{\"protobufBytes\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}},\n" +
                    "{\"finalMessage\":\"limit hit!\"}]"));
    verify(response, times(1)).write((String) any());
    verify(response, never()).write((Buffer) any());
    verify(vertx, times(1)).setTimer(anyLong(), any());
    verify(vertx).cancelTimer(anyLong());
  }
  @Test
  public void shouldConvertLogicalSchemaToProtobufSchema() {
    // Given:
    final String expectedProtoSchemaString = "syntax = \"proto3\";\n" +
            "\n" +
            "message ConnectDefault1 {\n" +
            "  int32 A = 1;\n" +
            "  double B = 2;\n" +
            "  repeated string C = 3;\n" +
            "}\n";

    // When:
    final String protoSchema = JsonStreamedRowResponseWriter.logicalToProtoSchema(SCHEMA);

    // Then:
    assertThat(protoSchema, is(expectedProtoSchemaString));
  }

  @Test
  public void shouldConvertComplexLogicalSchemaToProtobufSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K"), SqlTypes.struct()
                    .field("F1", SqlTypes.array(SqlTypes.STRING))
                    .build())
            .valueColumn(ColumnName.of("STR"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("DEC"), SqlTypes.decimal(4, 2))
            .valueColumn(ColumnName.of("BYTES_"), SqlTypes.BYTES)
            .valueColumn(ColumnName.of("ARRAY"), SqlTypes.array(SqlTypes.STRING))
            .valueColumn(ColumnName.of("MAP"), SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
            .valueColumn(ColumnName.of("STRUCT"), SqlTypes.struct().field("F1", SqlTypes.INTEGER).build())
            .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
                    .field("DECIMAL", SqlTypes.decimal(2, 1))
                    .field("STRUCT", SqlTypes.struct()
                            .field("F1", SqlTypes.STRING)
                            .field("F2", SqlTypes.INTEGER)
                            .build())
                    .field("ARRAY_STRUCT", SqlTypes.array(SqlTypes.struct().field("F1", SqlTypes.STRING).build()))
                    .field("ARRAY_MAP", SqlTypes.array(SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)))
                    .field("MAP_ARRAY", SqlTypes.map(SqlTypes.STRING, SqlTypes.array(SqlTypes.STRING)))
                    .field("MAP_MAP", SqlTypes.map(SqlTypes.STRING,
                            SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)
                    ))
                    .field("MAP_STRUCT", SqlTypes.map(SqlTypes.STRING,
                            SqlTypes.struct().field("F1", SqlTypes.STRING).build()
                    ))
                    .build()
            )
            .valueColumn(ColumnName.of("TIMESTAMP"), SqlTypes.TIMESTAMP)
            .valueColumn(ColumnName.of("DATE"), SqlTypes.DATE)
            .valueColumn(ColumnName.of("TIME"), SqlTypes.TIME)
            .headerColumn(ColumnName.of("HEAD"), Optional.of("h0"))
            .build();

    final String expectedProtoSchemaString = "syntax = \"proto3\";\n" +
            "\n" +
            "import \"confluent/type/decimal.proto\";\n" +
            "import \"google/protobuf/timestamp.proto\";\n" +
            "import \"google/type/date.proto\";\n" +
            "import \"google/type/timeofday.proto\";\n" +
            "\n" +
            "message ConnectDefault1 {\n" +
            "  ConnectDefault2 K = 1;\n" +
            "  string STR = 2;\n" +
            "  int64 LONG = 3;\n" +
            "  confluent.type.Decimal DEC = 4 [(confluent.field_meta) = {\n" +
            "    params: [\n" +
            "      {\n" +
            "        value: \"4\",\n" +
            "        key: \"precision\"\n" +
            "      },\n" +
            "      {\n" +
            "        value: \"2\",\n" +
            "        key: \"scale\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }];\n" +
            "  bytes BYTES_ = 5;\n" +
            "  repeated string ARRAY = 6;\n" +
            "  repeated ConnectDefault3Entry MAP = 7;\n" +
            "  ConnectDefault4 STRUCT = 8;\n" +
            "  ConnectDefault5 COMPLEX = 9;\n" +
            "  google.protobuf.Timestamp TIMESTAMP = 10;\n" +
            "  google.type.Date DATE = 11;\n" +
            "  google.type.TimeOfDay TIME = 12;\n" +
            "  bytes HEAD = 13;\n" +
            "\n" +
            "  message ConnectDefault2 {\n" +
            "    repeated string F1 = 1;\n" +
            "  }\n" +
            "  message ConnectDefault3Entry {\n" +
            "    string key = 1;\n" +
            "    string value = 2;\n" +
            "  }\n" +
            "  message ConnectDefault4 {\n" +
            "    int32 F1 = 1;\n" +
            "  }\n" +
            "  message ConnectDefault5 {\n" +
            "    confluent.type.Decimal DECIMAL = 1 [(confluent.field_meta) = {\n" +
            "      params: [\n" +
            "        {\n" +
            "          value: \"2\",\n" +
            "          key: \"precision\"\n" +
            "        },\n" +
            "        {\n" +
            "          value: \"1\",\n" +
            "          key: \"scale\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }];\n" +
            "    ConnectDefault6 STRUCT = 2;\n" +
            "    repeated ConnectDefault7 ARRAY_STRUCT = 3;\n" +
            "    repeated ConnectDefault8Entry ARRAY_MAP = 4;\n" +
            "    repeated ConnectDefault9Entry MAP_ARRAY = 5;\n" +
            "    repeated ConnectDefault10Entry MAP_MAP = 6;\n" +
            "    repeated ConnectDefault12Entry MAP_STRUCT = 7;\n" +
            "  \n" +
            "    message ConnectDefault6 {\n" +
            "      string F1 = 1;\n" +
            "      int32 F2 = 2;\n" +
            "    }\n" +
            "    message ConnectDefault7 {\n" +
            "      string F1 = 1;\n" +
            "    }\n" +
            "    message ConnectDefault8Entry {\n" +
            "      string key = 1;\n" +
            "      int32 value = 2;\n" +
            "    }\n" +
            "    message ConnectDefault9Entry {\n" +
            "      string key = 1;\n" +
            "      repeated string value = 2;\n" +
            "    }\n" +
            "    message ConnectDefault10Entry {\n" +
            "      string key = 1;\n" +
            "      repeated ConnectDefault11Entry value = 2;\n" +
            "    \n" +
            "      message ConnectDefault11Entry {\n" +
            "        string key = 1;\n" +
            "        int32 value = 2;\n" +
            "      }\n" +
            "    }\n" +
            "    message ConnectDefault12Entry {\n" +
            "      string key = 1;\n" +
            "      ConnectDefault13 value = 2;\n" +
            "    \n" +
            "      message ConnectDefault13 {\n" +
            "        string F1 = 1;\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    // When:
    final String protoSchema = JsonStreamedRowResponseWriter.logicalToProtoSchema(schema);

    // Then:
    assertThat(protoSchema, is(expectedProtoSchemaString));
  }

  @Test
  public void shouldFailNestedArraysConvertLogicalSchemaToProtobufSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
                    .field("ARRAY_ARRAY", SqlTypes.array(SqlTypes.array(SqlTypes.STRING)))
                    .build()
            )
            .build();

    // When:
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      JsonStreamedRowResponseWriter.logicalToProtoSchema(schema);
    });

    String expectedMessage = "Array cannot be nested";
    String actualMessage = exception.getMessage();

    // Then:
    assertThat(actualMessage.contains(expectedMessage), is(true));
  }
}