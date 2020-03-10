/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.StubInsertValuesExecutor.StubProducer;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.TopicInfoCache;
import io.confluent.ksql.test.tools.TopicInfoCache.TopicInfo;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class StubInsertValuesExecutorTest {

  private static final byte[] KEY_BYTES = "the-key".getBytes(StandardCharsets.UTF_8);
  private static final String SOME_TOPIC = "topic-name";

  @Mock
  private StubKafkaService stubKafkaService;
  @Mock
  private TopicInfoCache topicInfoCache;
  @Mock
  private TopicInfo topicInfo;
  @Captor
  private ArgumentCaptor<StubKafkaRecord> recordCaptor;
  private StubProducer stubProducer;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    when(stubKafkaService.getTopic(SOME_TOPIC)).thenReturn(new Topic(
        SOME_TOPIC,
        1,
        1,
        Optional.empty()
    ));

    when(topicInfoCache.get(SOME_TOPIC)).thenReturn(topicInfo);
    when(topicInfo.getKeyDeserializer()).thenReturn((Deserializer) new StringDeserializer());
    when(topicInfo.getValueDeserializer()).thenReturn((Deserializer) new StringDeserializer());

    stubProducer = new StubProducer(stubKafkaService, topicInfoCache);
  }

  @Test
  public void shouldWriteRecordKeyAndMetadata() {
    // Given:
    final long timestamp = 22L;
    final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        SOME_TOPIC,
        null,
        timestamp,
        KEY_BYTES,
        new byte[]{0}
    );

    // When:
    stubProducer.sendRecord(record);

    // Then:
    verify(stubKafkaService).writeRecord(eq(SOME_TOPIC), recordCaptor.capture());

    final Record actual = recordCaptor.getValue().getTestRecord();
    assertThat(actual.timestamp(), is(Optional.of(timestamp)));
    assertThat(actual.getWindow(), is(nullValue()));
    assertThat(actual.key(), is("the-key"));
  }

  @Test
  public void shouldWriteRecordStringValue() {
    // Given:
    final byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);

    final long timestamp = 22L;
    final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        SOME_TOPIC,
        null,
        timestamp,
        KEY_BYTES,
        value
    );

    // When:
    stubProducer.sendRecord(record);

    // Then:
    verify(stubKafkaService).writeRecord(eq(SOME_TOPIC), recordCaptor.capture());

    final Record actual = recordCaptor.getValue().getTestRecord();
    assertThat(actual.value(), is("the-value"));
  }
}