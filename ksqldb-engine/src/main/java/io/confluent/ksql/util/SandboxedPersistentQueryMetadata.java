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

package io.confluent.ksql.util;

/**
 * Sandboxed {@link PersistentQueryMetadata} that prevents to modify the state of the internal
 * {@link org.apache.kafka.streams.KafkaStreams}.
 */
public final class SandboxedPersistentQueryMetadata extends PersistentQueryMetadata {
  public static SandboxedPersistentQueryMetadata of(
      final PersistentQueryMetadata queryMetadata,
      final Listener listener
  ) {
    return new SandboxedPersistentQueryMetadata(queryMetadata, listener);
  }

  private SandboxedPersistentQueryMetadata(
      final PersistentQueryMetadata queryMetadata,
      final Listener listener
  ) {
    super(queryMetadata, listener);
  }

  @Override
  public void close() {
    closed = true;
    getListener().onClose(this);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  protected void closeKafkaStreams() {
    // no-op
  }
}
