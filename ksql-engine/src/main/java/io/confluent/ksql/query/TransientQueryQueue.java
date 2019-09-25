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

package io.confluent.ksql.query;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * A queue of rows for transient queries.
 */
class TransientQueryQueue {

  private final LimitQueueCallback callback;
  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue =
      new LinkedBlockingQueue<>(100);

  TransientQueryQueue(final KStream<?, GenericRow> kstream, final OptionalInt limit) {
    this.callback = limit.isPresent()
        ? new LimitedQueueCallback(limit.getAsInt())
        : new UnlimitedQueueCallback();

    kstream.foreach(new TransientQueryQueue.QueuePopulator<>(rowQueue, callback));
  }

  BlockingQueue<KeyValue<String, GenericRow>> getQueue() {
    return rowQueue;
  }

  void setLimitHandler(final LimitHandler limitHandler) {
    callback.setLimitHandler(limitHandler);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  static final class QueuePopulator<K> implements ForeachAction<K, GenericRow> {

    private final BlockingQueue<KeyValue<String, GenericRow>> queue;
    private final QueueCallback callback;

    QueuePopulator(
        final BlockingQueue<KeyValue<String, GenericRow>> queue,
        final QueueCallback callback
    ) {
      this.queue = Objects.requireNonNull(queue, "queue");
      this.callback = Objects.requireNonNull(callback, "callback");
    }

    @Override
    public void apply(final K key, final GenericRow row) {
      try {
        if (row == null) {
          return;
        }

        if (!callback.shouldQueue()) {
          return;
        }

        final String keyString = getStringKey(key);
        queue.put(new KeyValue<>(keyString, row));

        callback.onQueued();
      } catch (final InterruptedException exception) {
        throw new KsqlException("InterruptedException while enqueueing:" + key);
      }
    }

    private String getStringKey(final K key) {
      if (key instanceof Windowed) {
        final Windowed windowedKey = (Windowed) key;
        return String.format("%s : %s", windowedKey.key(), windowedKey.window());
      }

      return Objects.toString(key);
    }
  }
}
