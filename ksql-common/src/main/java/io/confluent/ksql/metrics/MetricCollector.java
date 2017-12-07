/**
 * Copyright 2017 Confluent Inc.
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
 **/
package io.confluent.ksql.metrics;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collection;
import java.util.Map;

interface MetricCollector extends ConsumerInterceptor, ProducerInterceptor {
  default ConsumerRecords onConsume(ConsumerRecords consumerRecords) {
    return consumerRecords;
  }

  default ProducerRecord onSend(ProducerRecord producerRecord) {
    return producerRecord;
  }

  default void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {  }

  default void close() {  }

  default void onCommit(Map map) {  }

  default void configure(Map<String, ?> map) {  }

  String getId();

  Collection<TopicSensors.Stat> stats(String topic, boolean isError);

  void recordError(String topic);
}
