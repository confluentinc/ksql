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
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.tools.test.model.Topic;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class StubKafkaServiceTest {

  @Mock
  private ParsedSchema avroKeySchema;
  @Mock
  private ParsedSchema avroSchema;
  @Mock
  private ProducerRecord<byte[], byte[]> producerRecord;

  private StubKafkaService kafka;
  private Topic topic;

  @Before
  public void setUp() {
    when(producerRecord.topic()).thenReturn("topic-name");

    kafka = StubKafkaService.create();

    topic = new Topic(producerRecord.topic(), Optional.of(avroKeySchema), Optional.of(avroSchema));
  }

  @Test
  public void shouldCreateTopicCorrectly() {
    // When:
    kafka.ensureTopic(topic);

    // Then:
    assertThat(kafka.getTopic(topic.getName()), is(topic));
  }

  @Test
  public void shouldUpdateTopic() {
    // Given:
    kafka.ensureTopic(topic);
    final Topic updatedTopic = new Topic(
        topic.getName(),
        topic.getNumPartitions() + 1,
        topic.getReplicas() + 1,
        topic.getKeySchemaId(),
        topic.getValueSchemaId(),
        topic.getKeySchema(),
        topic.getValueSchema(),
        topic.getKeySchemaReferences(),
        topic.getValueSchemaReferences(),
        topic.getKeyFeatures(),
        topic.getValueFeatures()
    );

    // When:
    kafka.ensureTopic(updatedTopic);

    // Then:
    assertThat(kafka.getTopic(topic.getName()), is(updatedTopic));
  }

  @Test
  public void shouldWriteReadSingleRecordToTopic() {
    // Given:
    kafka.ensureTopic(topic);

    // When:
    kafka.writeRecord(producerRecord);

    // Then:
    assertThat(kafka.readRecords(producerRecord.topic()), contains(producerRecord));
  }
}