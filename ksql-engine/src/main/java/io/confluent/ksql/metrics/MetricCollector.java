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

import io.confluent.common.metrics.Metrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Map;

public class MetricCollector implements AbstractMetricCollector {
  private ConsumerCollector consumerCollector;
  private ProducerCollector producerCollector;
  private final Date created = new Date();
  private String clientId;

  /**
   * Created by via registration as an Interceptor (hence the ugly binding to the MetricCollector)
   */
  public MetricCollector(){
  }

  @Override
  public ConsumerRecords onConsume(ConsumerRecords consumerRecords) {
    consumerCollector.onConsume(consumerRecords);
    return consumerRecords;
  }

  @Override
  public ProducerRecord onSend(ProducerRecord producerRecord) {
    return producerCollector.onSend(producerRecord);
  }

  @Override
  public void close() {
    MetricCollectors.remove(clientId);
    consumerCollector.close();
    producerCollector.close();
  }

  @Override
  public void configure(Map<String, ?> map) {
    this.clientId = map.containsKey(ConsumerConfig.GROUP_ID_CONFIG) ?
            (String) map.get(ConsumerConfig.GROUP_ID_CONFIG) : (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);

    Metrics metrics = MetricCollectors.addCollector(this);
    consumerCollector = new ConsumerCollector(metrics, this.clientId);
    producerCollector = new ProducerCollector(metrics, this.clientId);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "Created:" + created + " Client:" + clientId + " Consumer:" + consumerCollector.toString() + " Producer:" + producerCollector.toString();
  }

  public String getId() {
    return clientId;
  }

  public String statsForTopic(String topic) {
    String results = consumerCollector.statsForTopic(topic);
    return results + producerCollector.statsForTopic(topic);
  }
}
