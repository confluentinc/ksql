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
public final class SandboxedBinPackedPersistentQueryMetadataImpl
    extends BinPackedPersistentQueryMetadataImpl {
  public static SandboxedBinPackedPersistentQueryMetadataImpl of(
      final BinPackedPersistentQueryMetadataImpl queryMetadata,
      final QueryMetadata.Listener listener,
      final SandboxedSharedKafkaStreamsRuntimeImpl sandboxedRuntime
  ) {
    return new SandboxedBinPackedPersistentQueryMetadataImpl(queryMetadata, listener, sandboxedRuntime);
  }

  private SandboxedBinPackedPersistentQueryMetadataImpl(
      final BinPackedPersistentQueryMetadataImpl queryMetadata,
      final QueryMetadata.Listener listener,
      final SandboxedSharedKafkaStreamsRuntimeImpl sandboxedRuntime
  ) {
    super(queryMetadata, listener, sandboxedRuntime);
  }

  @Override
  public void stop() {
    //no-op
  }

  @Override
  public void close() {
    getListener().onClose(this);
  }

  @Override
  public void start() {
    // no-op
  }
}
