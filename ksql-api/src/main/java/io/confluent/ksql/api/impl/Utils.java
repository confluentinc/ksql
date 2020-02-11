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
import io.vertx.core.impl.VertxThread;

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
    checkThread(true);
  }

  public static void checkIsNotWorker() {
    checkThread(false);
  }

  public static boolean isEventLoopThread() {
    return isWorkerThread(false);
  }

  public static boolean isWorkerThread() {
    return isWorkerThread(true);
  }

  public static boolean isEventLoopAndSameContext(final Context context) {
    final Thread thread = Thread.currentThread();
    if (!(thread instanceof VertxThread)) {
      return false;
    }
    final VertxThread vertxThread = (VertxThread) thread;
    if (vertxThread.isWorker()) {
      return false;
    }
    return context == Vertx.currentContext();
  }

  private static boolean isWorkerThread(final boolean worker) {
    final Thread thread = Thread.currentThread();
    if (!(thread instanceof VertxThread)) {
      throw new IllegalStateException("Not a Vert.x thread " + thread);
    }
    final VertxThread vertxThread = (VertxThread) thread;
    return vertxThread.isWorker() == worker;
  }

  private static void checkThread(final boolean worker) {
    if (!isWorkerThread(worker)) {
      throw new IllegalStateException("Not a " + (worker ? "worker" : "event loop") + " thread");
    }
  }

  public static void checkContext(final Context context) {
    checkIsNotWorker();
    if (context != Vertx.currentContext()) {
      throw new IllegalStateException("On wrong context");
    }
  }

}
