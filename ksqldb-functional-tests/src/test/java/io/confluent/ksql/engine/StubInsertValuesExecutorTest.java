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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.StubInsertValuesExecutor.StubProducer;
import io.confluent.ksql.test.tools.TopicInfoCache;
import io.confluent.ksql.test.tools.TopicInfoCache.TopicInfo;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class StubInsertValuesExecutorTest {

  private static final String SOME_TOPIC = "topic-name";

  @Mock
  private StubKafkaService stubKafkaService;
  @Mock
  private TopicInfoCache topicInfoCache;
  @Mock
  private TopicInfo topicInfo;
  @Captor
  private ArgumentCaptor<ProducerRecord<?, ?>> recordCaptor;
  private StubProducer stubProducer;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    when(topicInfoCache.get(SOME_TOPIC)).thenReturn(topicInfo);
    when(topicInfo.getKeyDeserializer()).thenReturn((Deserializer) new LongDeserializer());
    when(topicInfo.getValueDeserializer()).thenReturn((Deserializer) new StringDeserializer());

    stubProducer = new StubProducer(stubKafkaService, topicInfoCache);
  }

  @Test
  public void shouldProduceRecordWithDeserializedKeyAndObject() {
    // Given:
    final long key = 1234L;
    final String value = "the-value";
    final long timestamp = 22L;
    final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        SOME_TOPIC,
        null,
        timestamp,
        serialize(key, new LongSerializer()),
        serialize(value, new StringSerializer())
    );

    // When:
    stubProducer.sendRecord(record);

    // Then:
    verify(stubKafkaService).writeRecord(recordCaptor.capture());

    assertThat(recordCaptor.getValue(), is(new ProducerRecord<>(
      SOME_TOPIC,
        null,
        timestamp,
        key,
        value
    )));
  }

  private static <T> byte[] serialize(final T v, final Serializer<T> serializer) {
    return serializer.serialize("", v);
  }
}