/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class QueryStreamWriterTest {

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Mock
  private TransientQueryMetadata queryMetadata;
  @Mock
  private BlockingRowQueue rowQueue;
  @Captor
  private ArgumentCaptor<StreamsUncaughtExceptionHandler> ehCapture;
  @Captor
  private ArgumentCaptor<LimitHandler> limitHandlerCapture;
  @Captor
  private ArgumentCaptor<CompletionHandler> completionHandlerCapture;
  private QueryStreamWriter writer;
  private ByteArrayOutputStream out;
  private LimitHandler limitHandler;
  private CompletionHandler completionHandler;
  private ObjectMapper objectMapper;

  @Before
  public void setUp() {

    objectMapper = ApiJsonMapper.INSTANCE.get();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("keyCol"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("col1"), SqlTypes.STRING)
        .build();

    when(queryMetadata.getQueryId()).thenReturn(new QueryId("id"));
    when(queryMetadata.getRowQueue()).thenReturn(rowQueue);
    when(queryMetadata.getLogicalSchema()).thenReturn(schema);
    when(queryMetadata.isRunning()).thenReturn(true);
    when(queryMetadata.getResultType()).thenReturn(ResultType.STREAM);
  }

  @Test
  public void shouldWriteAnyPendingRowsBeforeReportingException() {
    // Given:
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(rowQueue).drainTo(any());

    createWriter();

    givenUncaughtException(new KsqlException("Server went Boom"));

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3"),
        containsString("Server went Boom")
    ));
  }

  @Test
  public void shouldExitAndDrainIfQueryStopsRunning() {
    // Given:
    when(queryMetadata.isRunning()).thenReturn(false);
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(rowQueue).drainTo(any());

    createWriter();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, is(Arrays.asList(
        "[{\"header\":{\"queryId\":\"id\",\"schema\":\"`col1` STRING\"}},",
        "{\"row\":{\"columns\":[\"Row1\"]}},",
        "{\"row\":{\"columns\":[\"Row2\"]}},",
        "{\"row\":{\"columns\":[\"Row3\"]}},",
        "]"
    )));
  }

  @Test
  public void shouldExitAndDrainIfQueryComplete() {
    // Given:
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(rowQueue).drainTo(any());

    writer = new QueryStreamWriter(
        queryMetadata,
        1000,
        objectMapper,
        new CompletableFuture<>()
    );

    out = new ByteArrayOutputStream();

    verify(queryMetadata).setCompletionHandler(completionHandlerCapture.capture());
    completionHandler = completionHandlerCapture.getValue();
    completionHandler.complete();
    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, is(Arrays.asList(
        "[{\"header\":{\"queryId\":\"id\",\"schema\":\"`col1` STRING\"}},",
        "{\"row\":{\"columns\":[\"Row1\"]}},",
        "{\"row\":{\"columns\":[\"Row2\"]}},",
        "{\"row\":{\"columns\":[\"Row3\"]}},",
        "{\"finalMessage\":\"Query Completed\"}]"
    )));
  }

  @Test
  public void shouldExitAndDrainIfLimitReached() {
    // Given:
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(rowQueue).drainTo(any());

    createWriter();

    limitHandler.limitReached();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  @Test
  public void shouldHandleTableRows() {
    // Given:
    when(queryMetadata.getResultType()).thenReturn(ResultType.TABLE);

    doAnswer(tableRows("key1", "Row1", "key2", null, "key3", "Row3"))
        .when(rowQueue).drainTo(any());

    createWriter();

    forceWriterToNotBlock();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("{\"row\":{\"columns\":[\"Row1\"]}}"),
        containsString("{\"row\":{\"columns\":[null],\"tombstone\":true}}"),
        containsString("{\"row\":{\"columns\":[\"Row3\"]}}")
    ));
  }

  @Test
  public void shouldHandleWindowedTableRows() {
    // Given:
    when(queryMetadata.getResultType()).thenReturn(ResultType.WINDOWED_TABLE);

    doAnswer(windowedTableRows("key1", "Row1", "key2", null, "key3", "Row3"))
        .when(rowQueue).drainTo(any());

    createWriter();

    forceWriterToNotBlock();

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("{\"header\":{\"queryId\":\"id\",\"schema\":\"`col1` STRING\"}}"),
        containsString("{\"row\":{\"columns\":[\"Row1\"]}}"),
        containsString("{\"row\":{\"columns\":[null],\"tombstone\":true}}"),
        containsString("{\"row\":{\"columns\":[\"Row3\"]}}")
    ));
  }

  private void forceWriterToNotBlock() {
    limitHandler.limitReached();
  }

  private void createWriter() {
    writer = new QueryStreamWriter(queryMetadata, 1000, objectMapper, new CompletableFuture<>()
    );

    out = new ByteArrayOutputStream();

    verify(queryMetadata).setLimitHandler(limitHandlerCapture.capture());
    limitHandler = limitHandlerCapture.getValue();
  }

  private void givenUncaughtException(final KsqlException e) {
    verify(queryMetadata).setUncaughtExceptionHandler(ehCapture.capture());
    ehCapture.getValue().handle(e);
  }

  private static Answer<Void> streamRows(final Object... rows) {
    return inv -> {
      final Collection<KeyValueMetadata<List<Object>, GenericRow>> output = inv.getArgument(0);

      Arrays.stream(rows)
          .map(GenericRow::genericRow)
          .map(value -> new KeyValueMetadata<>(KeyValue.keyValue((List<Object>) null, value)))
          .forEach(output::add);

      return null;
    };
  }

  private static Answer<Void> tableRows(final Object... rows) {
    return inv -> {
      final Collection<KeyValueMetadata<List<Object>, GenericRow>> output = inv.getArgument(0);

      for (int i = 0; i < rows.length; i = i + 2) {
        final List<Object> key = ImmutableList.of(rows[i]);
        final GenericRow value = rows[i + 1] == null
            ? null
            : GenericRow.genericRow(rows[i + 1]);

        output.add(new KeyValueMetadata<>(KeyValue.keyValue(key, value)));
      }

      return null;
    };
  }

  private static Answer<Void> windowedTableRows(final Object... rows) {
    return inv -> {
      final Collection<KeyValueMetadata<List<Object>, GenericRow>> output = inv.getArgument(0);

      for (int i = 0; i < rows.length; i = i + 2) {
        final List<Object> key = ImmutableList.of(rows[i], 1000, 2000);
        final GenericRow value = rows[i + 1] == null
            ? null
            : GenericRow.genericRow(rows[i + 1]);

        output.add(new KeyValueMetadata<>(KeyValue.keyValue(key, value)));
      }

      return null;
    };
  }

  private static List<String> getOutput(final ByteArrayOutputStream out) {
    final String[] lines = new String(out.toByteArray(), StandardCharsets.UTF_8).split("\n");
    return Arrays.stream(lines)
        .filter(line -> !line.isEmpty())
        .collect(Collectors.toList());
  }
}
