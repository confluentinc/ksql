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
      final QueryMetadata.Listener listener
  ) {
    return new SandboxedBinPackedPersistentQueryMetadataImpl(queryMetadata, listener);
  }

  private SandboxedBinPackedPersistentQueryMetadataImpl(
      final BinPackedPersistentQueryMetadataImpl queryMetadata,
      final QueryMetadata.Listener listener
  ) {
    super(queryMetadata, listener);
  }

  @Override
  public void pause() {
    // no-op
  }

  @Override
  public void resume() {
    // no-op
  }

  @Override
  public void stop() {
    //no-op
  }

  @Override
  public void stop(final boolean resetOffsets) {
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
