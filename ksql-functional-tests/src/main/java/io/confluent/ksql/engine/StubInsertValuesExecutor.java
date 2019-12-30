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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.TopicInfoCache;
import io.confluent.ksql.test.tools.TopicInfoCache.TopicInfo;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class StubInsertValuesExecutor {

  private StubInsertValuesExecutor() {
  }

  public static InsertValuesExecutor of(
      final StubKafkaService stubKafkaService,
      final KsqlExecutionContext executionContext
  ) {
    final StubProducer stubProducer = new StubProducer(stubKafkaService, executionContext);

    return new InsertValuesExecutor(
        false,
        (record, ignored1, ignored2) -> stubProducer.sendRecord(record));
  }

  @VisibleForTesting
  static class StubProducer {

    private final StubKafkaService stubKafkaService;
    private final TopicInfoCache topicInfoCache;

    StubProducer(
        final StubKafkaService stubKafkaService,
        final KsqlExecutionContext executionContext
    ) {
      this.stubKafkaService = Objects.requireNonNull(stubKafkaService, "stubKafkaService");
      this.topicInfoCache = new TopicInfoCache(
          executionContext,
          executionContext.getServiceContext().getSchemaRegistryClient()
      );
    }

    void sendRecord(final ProducerRecord<byte[], byte[]> record) {
      final Topic topic = stubKafkaService.getTopic(record.topic());

      final Object key = deserializeKey(record);

      final Object value = deserializeValue(record);

      final Optional<Long> timestamp = Optional.of(record.timestamp());

      this.stubKafkaService.writeRecord(record.topic(),
          StubKafkaRecord.of(
              new Record(
                  topic,
                  key,
                  value,
                  null,
                  timestamp,
                  null
              ),
              null)
      );
    }

    private Object deserializeKey(final ProducerRecord<byte[], byte[]> record) {
      try {
        final TopicInfo topicInfo = topicInfoCache.get(record.topic());
        return topicInfo.getKeyDeserializer()
            .deserialize(record.topic(), record.key());
      } catch (final Exception e) {
        throw new InvalidFieldException("key", "failed to parse", e);
      }
    }

    private Object deserializeValue(final ProducerRecord<byte[], byte[]> record) {
      try {
        final TopicInfo topicInfo = topicInfoCache.get(record.topic());
        return topicInfo.getValueDeserializer()
            .deserialize(record.topic(), record.value());
      } catch (final Exception e) {
        throw new InvalidFieldException("value", "failed to parse", e);
      }
    }
  }
}
