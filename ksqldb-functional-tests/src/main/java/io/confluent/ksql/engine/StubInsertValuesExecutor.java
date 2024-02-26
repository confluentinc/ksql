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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.server.execution.InsertValuesExecutor;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class StubInsertValuesExecutor {

  private StubInsertValuesExecutor() {
  }

  public static InsertValuesExecutor of(final StubKafkaService stubKafkaService) {
    final StubProducer stubProducer = new StubProducer(stubKafkaService);

    return new InsertValuesExecutor(
        false,
        (record, ignored1, ignored2) -> stubProducer.sendRecord(record)
    );
  }

  @VisibleForTesting
  static class StubProducer {

    private final StubKafkaService stubKafkaService;

    StubProducer(final StubKafkaService stubKafkaService) {
      this.stubKafkaService = requireNonNull(stubKafkaService, "stubKafkaService");
    }

    void sendRecord(final ProducerRecord<byte[], byte[]> record) {
      this.stubKafkaService.writeRecord(record);
    }
  }
}
