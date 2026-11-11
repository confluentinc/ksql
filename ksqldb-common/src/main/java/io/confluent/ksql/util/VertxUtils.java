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

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.internal.ContextInternal;

/**
 * General purpose utils (not limited to the server, could be used by client too) for the API
 * module.
 */
public final class VertxUtils {

  private VertxUtils() {
  }

  public static void checkIsWorker() {
    if (!Context.isOnWorkerThread()) {
      throw new IllegalStateException("Not a worker thread");
    }
  }

  public static void checkContext(final Context context) {
    if (!isEventLoopAndSameContext(context)) {
      throw new IllegalStateException("On wrong context or worker");
    }
  }

  public static boolean isEventLoopAndSameContext(final Context context) {
    return Context.isOnEventLoopThread()
        && (context == Vertx.currentContext()
        || checkDuplicateContext(Vertx.currentContext(), context)
        || checkDuplicateContext(context, Vertx.currentContext()));
  }

  /**
   * Runs {@code blockingCode} on {@code executor}, passing it a promise it may complete
   * later, possibly from another thread.
   *
   * <p>Vert.x 5 removed the {@code executeBlocking(Handler<Promise<T>>, ...)} variants, and
   * its {@code Callable} variants must produce the result synchronously. This keeps the
   * Vert.x 4 behaviour: the returned future completes when the promise does (or fails if
   * {@code blockingCode} throws), and its callbacks run on the caller's context.
   */
  public static <T> Future<T> executeBlocking(
      final WorkerExecutor executor,
      final Handler<Promise<T>> blockingCode,
      final boolean ordered
  ) {
    final Promise<T> promise = Promise.promise();
    return executor.<Void>executeBlocking(() -> {
      blockingCode.handle(promise);
      return null;
    }, ordered).compose(v -> promise.future());
  }

  private static boolean checkDuplicateContext(final Context context, final Context other) {
    // see https://github.com/eclipse-vertx/vert.x/issues/3300 - the recommendation from
    // the VertX community is to always call runOnContext() despite the performance overhead
    // instead of checking the context. this hack allows us to keep the same pattern we had
    // from before the VertX 4 migration
    return context instanceof ContextInternal
        && ((ContextInternal) context).isDuplicate()
        && (((ContextInternal) context).unwrap() == other);
  }

}
