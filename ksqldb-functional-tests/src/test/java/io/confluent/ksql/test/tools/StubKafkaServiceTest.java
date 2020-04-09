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

package io.confluent.ksql.test.tools;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StubKafkaServiceTest {

  @Mock
  private ParsedSchema avroSchema;
  @Mock
  private ProducerRecord<String, String> producerRecord;

  private StubKafkaRecord stubKafkaRecord;

  private StubKafkaService stubKafkaService;
  private Topic topic;

  @Before
  public void setUp() {
    when(producerRecord.topic()).thenReturn("topic-name");
    when(producerRecord.key()).thenReturn("key");
    when(producerRecord.value()).thenReturn("value");

    stubKafkaRecord = StubKafkaRecord.of(producerRecord);

    stubKafkaService = StubKafkaService.create();

    topic = new Topic("foo", 1, 1, Optional.of(avroSchema));
  }

  @Test
  public void shouldCreateTopicCorrectly() {
    // When:
    stubKafkaService.createTopic(topic);

    // Then:
    stubKafkaService.requireTopicExists(topic.getName());
  }

  @Test
  public void shouldWriteSingleRecordToTopic() {
    // Given:
    stubKafkaService.createTopic(topic);

    // When:
    stubKafkaService.writeRecord("foo", stubKafkaRecord);

    // Then:
    assertThat(stubKafkaService.getTopicData().get("foo").get(0), is(stubKafkaRecord));
  }

  @Test
  public void shouldReadRecordFromTopic() {
    // Given:
    stubKafkaService.createTopic(topic);
    stubKafkaService.writeRecord("foo", stubKafkaRecord);

    // When:
    final List<StubKafkaRecord> records = stubKafkaService.readRecords(topic.getName());

    // Then:
    assertThat(records.size(), CoreMatchers.equalTo(1));
    assertThat(records.get(0), is(stubKafkaRecord));
  }
}