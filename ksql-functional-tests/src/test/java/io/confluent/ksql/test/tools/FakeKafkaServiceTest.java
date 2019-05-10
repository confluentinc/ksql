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
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FakeKafkaServiceTest {

  @Mock
  private Schema avroSchema;
  @Mock
  private ProducerRecord producerRecord;
  @Mock
  private Record record;
  private final FakeKafkaRecord fakeKafkaRecord = new FakeKafkaRecord(record, producerRecord);


  @Test
  public void shouldCreateTopicCorrectly() {
    // Given:
    final FakeKafkaService fakeKafkaService = FakeKafkaService.create();
    final Topic topic = new Topic("foo", Optional.of(avroSchema), new StringSerdeSupplier(), 1, 1);

    // When:
    fakeKafkaService.createTopic(topic);

    // Then:
    assertTrue(fakeKafkaService.getTopicMap().containsKey(topic.getName()));
  }

  @Test
  public void shouldWriteSingleRecordToTopic() {
    // Givien:
    final FakeKafkaService fakeKafkaService = FakeKafkaService.create();
    final Topic topic = new Topic("foo", Optional.of(avroSchema), new StringSerdeSupplier(), 1, 1);
    fakeKafkaService.createTopic(topic);

    // When:
    fakeKafkaService.writeRecord("foo", fakeKafkaRecord);

    // Then:
    assertThat(fakeKafkaService.getTopicData().get("foo").get(0), is(fakeKafkaRecord));
  }


  public void shouldReadRecordFromTopic() {
    // Givien:
    final FakeKafkaService fakeKafkaService = FakeKafkaService.create();
    final Topic topic = new Topic("foo", Optional.of(avroSchema), new StringSerdeSupplier(), 1, 1);
    fakeKafkaService.createTopic(topic);
    fakeKafkaService.writeRecord("foo", fakeKafkaRecord);

    // When:
    final List<FakeKafkaRecord> records = fakeKafkaService.readRecords(topic.getName());

    // Then:
    assertThat(records.size(), CoreMatchers.equalTo(1));
    assertThat(records.get(0), is(fakeKafkaRecord));

  }


}