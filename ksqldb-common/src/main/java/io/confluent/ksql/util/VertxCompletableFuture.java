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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of CompletableFuture which also implements a Vert.x Handler of AsyncResult, so
 * it can be passed to Vert.x methods which expect that.
 */
public class VertxCompletableFuture<T> extends CompletableFuture<T> implements
    Handler<AsyncResult<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(VertxCompletableFuture.class);
  public final boolean debug;

  public VertxCompletableFuture() {
    debug = false;
  }

  public VertxCompletableFuture(boolean debug) {
    this.debug = debug;
    if (debug) {
      LOG.info("Created VCF with id " + this, new Throwable());
    }
  }

  @Override
  public void handle(final AsyncResult<T> ar) {
    if (debug) {
      LOG.info("Handling ar for VCF with id " + this, new Throwable());
    }
    if (ar.succeeded()) {
      complete(ar.result());
    } else {
      completeExceptionally(ar.cause());
    }
  }

}

