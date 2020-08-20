/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CommandTopicBackupNoOp implements CommandTopicBackup {
  @Override
  public void initialize() {
    // no-op
  }

  @Override
  public void writeRecord(final ConsumerRecord<byte[], byte[]> record) {
    // no-op
  }

  @Override
  public void close() {
    // no-op
  }
}
