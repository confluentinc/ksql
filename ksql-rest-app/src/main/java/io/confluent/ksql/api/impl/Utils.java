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

package io.confluent.ksql.api.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * General purpose utils (not limited to the server, could be used by client too) for the API
 * module.
 */
public final class Utils {

  private Utils() {
  }

  /**
   * Connects promise to the future. I.e. when the future completes it causes the promise to
   * complete.
   *
   * @param future  The future
   * @param promise The promise
   * @param <T>     The type of the result
   */
  public static <T> void connectPromise(final Future<T> future, final Promise<T> promise) {
    future.setHandler(ar -> {
      if (ar.succeeded()) {
        promise.complete(ar.result());
      } else {
        promise.fail(ar.cause());
      }
    });
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
    return Context.isOnEventLoopThread() && context == Vertx.currentContext();
  }

}
