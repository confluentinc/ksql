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

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
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
@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class QueryStreamWriterTest {

  @Mock(MockType.NICE)
  private KsqlEngine ksqlEngine;
  private Capture<Thread.UncaughtExceptionHandler> ehCapture;
  private BlockingQueue<KeyValue<String, GenericRow>> rowQueue;

  @Before
  public void setUp() throws Exception {
    rowQueue = new LinkedBlockingQueue<>(100);
    ehCapture = newCapture();

    Schema schema = SchemaBuilder.struct().field("col1", Schema.STRING_SCHEMA).build();

    final KafkaStreams kStreams = niceMock(KafkaStreams.class);
    final QueuedQueryMetadata queryMetadata = niceMock(QueuedQueryMetadata.class);

    kStreams.setUncaughtExceptionHandler(capture(ehCapture));
    expectLastCall();

    expect(queryMetadata.getKafkaStreams()).andReturn(kStreams).anyTimes();
    expect(queryMetadata.getRowQueue()).andReturn(rowQueue).anyTimes();
    expect(queryMetadata.getResultSchema()).andReturn(schema).anyTimes();

    expect(ksqlEngine.buildMultipleQueries(anyObject(), anyObject()))
        .andReturn(ImmutableList.of(queryMetadata));


    replay(queryMetadata, kStreams, ksqlEngine);
  }

  @Test
  public void shouldWriteAnyPendingRowsBeforeReportingException() throws Exception {
    // Given:
    final QueryStreamWriter writer = new QueryStreamWriter(ksqlEngine,
                                                           1000,
                                                           "a KSQL statement",
                                                           Collections.emptyMap());

    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    givenPendingRows("Row1", "Row2", "Row3");
    givenUncaughtException(new KsqlException("LIMIT reached for the partition."));

    // When:
    writer.write(out);

    // Then:
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  private static List<String> getOutput(final ByteArrayOutputStream out) {
    final String[] lines = new String(out.toByteArray(), StandardCharsets.UTF_8).split("\n");
    return Arrays.stream(lines)
        .filter(line -> !line.isEmpty())
        .collect(Collectors.toList());
  }

  private void givenPendingRows(final Object... rows) {
    Arrays.stream(rows)
        .map(ImmutableList::of)
        .map(GenericRow::new)
        .forEach(row -> rowQueue.offer(new KeyValue<>("no used", row)));
  }

  private void givenUncaughtException(final KsqlException e) {
    ehCapture.getValue().uncaughtException(new Thread(), e);
  }
}