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

import io.confluent.ksql.query.QueryError;
import org.apache.kafka.streams.KafkaStreams.State;

/**
 * Sandboxed {@link PersistentQueryMetadata} that prevents to modify the state of the internal
 * {@link org.apache.kafka.streams.KafkaStreams}.
 */
public final class SandboxedSharedRuntimePersistentQueryMetadata
    extends SharedRuntimePersistentQueryMetadata {

  public static SandboxedSharedRuntimePersistentQueryMetadata of(
      final SharedRuntimePersistentQueryMetadata queryMetadata
  ) {
    return new SandboxedSharedRuntimePersistentQueryMetadata(queryMetadata, new SandboxListener());
  }

  private SandboxedSharedRuntimePersistentQueryMetadata(
      final SharedRuntimePersistentQueryMetadata queryMetadata,
      final QueryMetadata.Listener listener
  ) {
    super(queryMetadata, listener);
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

  static class SandboxListener implements QueryMetadata.Listener {

    @Override
    public void onError(final QueryMetadata queryMetadata, final QueryError error) {
    }

    @Override
    public void onStateChange(
        final QueryMetadata queryMetadata,
        final State before,
        final State after) {
    }

    @Override
    public void onClose(final QueryMetadata queryMetadata) {
    }
  }
}
