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
public final class SandboxedPersistentQueriesInSharedRuntimesImpl
    extends PersistentQueriesInSharedRuntimesImpl {
  public static SandboxedPersistentQueriesInSharedRuntimesImpl of(
      final PersistentQueriesInSharedRuntimesImpl queryMetadata,
      final QueryMetadata.Listener listener
  ) {
    return new SandboxedPersistentQueriesInSharedRuntimesImpl(queryMetadata, listener);
  }

  private SandboxedPersistentQueriesInSharedRuntimesImpl(
      final PersistentQueriesInSharedRuntimesImpl queryMetadata,
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
}
