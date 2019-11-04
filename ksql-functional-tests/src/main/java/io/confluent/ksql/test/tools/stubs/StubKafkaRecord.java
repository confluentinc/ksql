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

package io.confluent.ksql.test.tools.stubs;

import io.confluent.ksql.test.model.WindowData;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.Windowed;

public final class StubKafkaRecord {

  private final Record testRecord;
  private final ProducerRecord<?,?> producerRecord;

  private StubKafkaRecord(final Record testRecord, final ProducerRecord<?,?> producerRecord) {
    this.testRecord = testRecord;
    this.producerRecord = producerRecord;
  }

  public static StubKafkaRecord of(
      final Record testRecord,
      final ProducerRecord<?,?> producerRecord
  ) {
    Objects.requireNonNull(testRecord, "testRecord");
    return new StubKafkaRecord(testRecord, producerRecord);
  }

  public static StubKafkaRecord of(
      final Topic topic,
      final ProducerRecord<?,?> producerRecord) {
    Objects.requireNonNull(producerRecord);
    Objects.requireNonNull(topic, "topic");
    final Record testRecord = new Record(
        topic,
        Objects.toString(producerRecord.key()),
        producerRecord.value(),
        null,
        Optional.of(producerRecord.timestamp()),
        getWindowData(producerRecord)
    );
    return new StubKafkaRecord(testRecord, producerRecord);
  }

  private static WindowData getWindowData(
      final ProducerRecord<?,?> producerRecord) {
    if (producerRecord.key() instanceof Windowed) {
      final Windowed<?> windowed = (Windowed<?>) producerRecord.key();
      return new WindowData(windowed);
    }
    return null;
  }

  public Record getTestRecord() {
    return testRecord;
  }

  public ProducerRecord<?,?> getProducerRecord() {
    return producerRecord;
  }
}
