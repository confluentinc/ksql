/*
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
 */

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.rest.util.StructSerializationModule;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.IAnswer;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueuedQueryMetadata;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author andy
 * created 19/04/2018
 */
@SuppressWarnings({"unchecked", "ConstantConditions"})
@RunWith(EasyMockRunner.class)
public class QueryStreamWriterTest {
  @ClassRule
  public static final Timeout TIMEOUT = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Mock(MockType.NICE)
  private KsqlEngine ksqlEngine;
  @Mock(MockType.NICE)
  private QueuedQueryMetadata queryMetadata;
  @Mock(MockType.NICE)
  private BlockingQueue<KeyValue<String, GenericRow>> rowQueue;
  private Capture<Thread.UncaughtExceptionHandler> ehCapture;
  private Capture<Collection<KeyValue<String, GenericRow>>> drainCapture;
  private Capture<OutputNode.LimitHandler> limitHandlerCapture;
  private QueryStreamWriter writer;
  private ByteArrayOutputStream out;
  private OutputNode.LimitHandler limitHandler;
  private ObjectMapper objectMapper;

  @Before
  public void setUp() {

    objectMapper = JsonMapper.INSTANCE.mapper;

    ehCapture = newCapture();
    drainCapture = newCapture();
    limitHandlerCapture = newCapture();

    Schema schema = SchemaBuilder.struct().field("col1", Schema.OPTIONAL_STRING_SCHEMA).build();

    final KafkaStreams kStreams = niceMock(KafkaStreams.class);

    kStreams.setUncaughtExceptionHandler(capture(ehCapture));
    expectLastCall();

    expect(queryMetadata.getKafkaStreams()).andReturn(kStreams).anyTimes();
    expect(queryMetadata.getRowQueue()).andReturn(rowQueue).anyTimes();
    expect(queryMetadata.getResultSchema()).andReturn(schema).anyTimes();

    expect(ksqlEngine.buildMultipleQueries(anyObject(), anyObject(), anyObject()))
        .andReturn(ImmutableList.of(queryMetadata));

    queryMetadata.setLimitHandler(capture(limitHandlerCapture));
    expectLastCall().once();

    replay(kStreams);
  }

  @Test
  public void shouldWriteAnyPendingRowsBeforeReportingException() throws Exception {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).anyTimes();
    expect(rowQueue.drainTo(capture(drainCapture))).andAnswer(rows("Row1", "Row2", "Row3"));

    createWriter();

    givenUncaughtException(new KsqlException("Server went Boom"));

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
  public void shouldExitAndDrainIfQueryStopsRunning() throws Exception {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).andReturn(false);
    expect(rowQueue.drainTo(capture(drainCapture))).andAnswer(rows("Row1", "Row2", "Row3"));

    createWriter();

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
  public void shouldExitAndDrainIfLimitReached() throws Exception {
    // Given:
    expect(queryMetadata.isRunning()).andReturn(true).anyTimes();
    expect(rowQueue.drainTo(capture(drainCapture))).andAnswer(rows("Row1", "Row2", "Row3"));

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

  private void createWriter() throws Exception {
    replay(queryMetadata, ksqlEngine, rowQueue);

    writer = new QueryStreamWriter(
        new KsqlConfig(Collections.emptyMap()),
        ksqlEngine,
        1000,
        "a KSQL statement",
        Collections.emptyMap(),
        objectMapper
        );

    out = new ByteArrayOutputStream();
    limitHandler = limitHandlerCapture.getValue();
  }

  private void givenUncaughtException(final KsqlException e) {
    ehCapture.getValue().uncaughtException(new Thread(), e);
  }

  private IAnswer<Integer> rows(final Object... rows) {
    return () -> {
      final Collection<KeyValue<String, GenericRow>> output = drainCapture.getValue();

      Arrays.stream(rows)
          .map(ImmutableList::of)
          .map(GenericRow::new)
          .forEach(row -> output.add(new KeyValue<>("no used", row)));

      return rows.length;
    };
  }

  private static List<String> getOutput(final ByteArrayOutputStream out) {
    final String[] lines = new String(out.toByteArray(), StandardCharsets.UTF_8).split("\n");
    return Arrays.stream(lines)
        .filter(line -> !line.isEmpty())
        .collect(Collectors.toList());
  }
}
