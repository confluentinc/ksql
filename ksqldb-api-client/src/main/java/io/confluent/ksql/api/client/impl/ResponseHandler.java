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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

abstract class ResponseHandler<T extends CompletableFuture<?>> {

  protected final Context context;
  protected final JsonParser recordParser;
  protected final T cf;

  ResponseHandler(final Context context, final JsonParser recordParser, final T cf) {
    this.context = Objects.requireNonNull(context);
    this.recordParser = Objects.requireNonNull(recordParser);
    this.cf = Objects.requireNonNull(cf);
  }

  public void handleBodyBuffer(final JsonEvent buff) {
    checkContext();
    doHandleBodyBuffer(buff);
  }

  public void handleException(final Throwable t) {
    checkContext();
    doHandleException(t);
  }

  public void handleBodyEnd(final Void v) {
    checkContext();
    doHandleBodyEnd();
  }

  protected abstract void doHandleBodyBuffer(JsonEvent buff);

  protected abstract void doHandleException(Throwable t);

  protected abstract void doHandleBodyEnd();

  protected void checkContext() {
    VertxUtils.checkContext(context);
  }
}
