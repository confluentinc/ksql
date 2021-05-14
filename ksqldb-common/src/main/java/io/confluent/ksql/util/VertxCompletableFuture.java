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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of CompletableFuture which also implements a Vert.x Handler of AsyncResult, so
 * it can be passed to Vert.x methods which expect that.
 */
public class VertxCompletableFuture<T> extends CompletableFuture<T> implements
    Handler<AsyncResult<T>> {

  public VertxCompletableFuture() {
  }

  @Override
  public void handle(final AsyncResult<T> ar) {
    if (ar.succeeded()) {
      complete(ar.result());
    } else {
      completeExceptionally(ar.cause());
    }
  }

}

