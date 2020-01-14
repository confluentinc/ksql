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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * A queue of rows for transient queries.
 */
class TransientQueryQueue implements BlockingRowQueue {

  private final LimitQueueCallback callback;
  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue;
  private final int offerTimeoutMs;
  private volatile boolean closed = false;

  TransientQueryQueue(final KStream<?, GenericRow> kstream, final OptionalInt limit) {
    this(kstream, limit, 100, 100);
  }

  @VisibleForTesting
  TransientQueryQueue(
      final KStream<?, GenericRow> kstream,
      final OptionalInt limit,
      final int queueSizeLimit,
      final int offerTimeoutMs
  ) {
    this.callback = limit.isPresent()
        ? new LimitedQueueCallback(limit.getAsInt())
        : new UnlimitedQueueCallback();
    this.rowQueue = new LinkedBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;

    kstream.foreach(new QueuePopulator<>());
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    callback.setLimitHandler(limitHandler);
  }

  @Override
  public KeyValue<String, GenericRow> poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  @Override
  public void drainTo(final Collection<? super KeyValue<String, GenericRow>> collection) {
    rowQueue.drainTo(collection);
  }

  @Override
  public int size() {
    return rowQueue.size();
  }

  @Override
  public void close() {
    closed = true;
  }

  @VisibleForTesting
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  final class QueuePopulator<K> implements ForeachAction<K, GenericRow> {

    @Override
    public void apply(final K key, final GenericRow row) {
      try {
        if (row == null) {
          return;
        }

        if (!callback.shouldQueue()) {
          return;
        }

        final KeyValue<String, GenericRow> kv = new KeyValue<>(getStringKey(key), row);

        while (!closed) {
          if (rowQueue.offer(kv, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onQueued();
            break;
          }
        }
      } catch (final InterruptedException e) {
        throw new KsqlException("InterruptedException while enqueueing:" + key);
      }
    }

    private String getStringKey(final K key) {
      if (key instanceof Windowed) {
        final Windowed<?> windowedKey = (Windowed<?>) key;
        return String.format("%s : %s", windowedKey.key(), windowedKey.window());
      }

      return Objects.toString(key);
    }
  }
}
