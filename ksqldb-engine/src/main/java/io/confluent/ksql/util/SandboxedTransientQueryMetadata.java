/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.LimitHandler;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class SandboxedTransientQueryMetadata extends TransientQueryMetadata {
  private SandboxedTransientQueryMetadata(
      final TransientQueryMetadata actual,
      final Listener listener
  ) {
    super(actual, new SandboxQueue(), listener);
  }

  public static SandboxedTransientQueryMetadata of(
      final TransientQueryMetadata queryMetadata,
      final QueryMetadata.Listener listener
  ) {
    return new SandboxedTransientQueryMetadata(
        Objects.requireNonNull(queryMetadata, "queryMetadata"),
        Objects.requireNonNull(listener, "listener")
    );
  }

  @Override
  public void initialize() {
    // no-op
  }

  @Override
  public void start() {
    throw new IllegalStateException("SandboxedTransientQueryMetadta should never be started");
  }

  @Override
  public void close() {
    isRunning.set(false);
    getListener().onClose(this);
  }

  private static class SandboxQueue implements BlockingRowQueue {
    private static void throwUseException() {
      throw new IllegalStateException("SandboxedTransientQueryMetadata should never use queue");
    }

    public void setLimitHandler(final LimitHandler limitHandler) {
      throwUseException();
    }

    @Override
    public void setCompletionHandler(final CompletionHandler completionHandler) {
      throwUseException();
    }

    public void setQueuedCallback(final Runnable callback) {
      throwUseException();
    }

    public KeyValueMetadata<List<?>, GenericRow> poll(final long timeout, final TimeUnit unit) {
      throwUseException();
      return null;
    }

    public KeyValueMetadata<List<?>, GenericRow> poll() {
      throwUseException();
      return null;
    }

    public void drainTo(
        final Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection) {
      throwUseException();
    }

    public int size() {
      throwUseException();
      return -1;
    }

    public boolean isEmpty() {
      throwUseException();
      return false;
    }

    public void close() {
      throwUseException();
    }
  }
}
