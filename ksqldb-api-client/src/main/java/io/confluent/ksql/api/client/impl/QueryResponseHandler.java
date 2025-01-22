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

import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import java.util.concurrent.CompletableFuture;

abstract class QueryResponseHandler<T extends CompletableFuture<?>> extends ResponseHandler<T> {

  protected boolean hasReadArguments;

  QueryResponseHandler(final Context context, final RecordParser recordParser, final T cf) {
    super(context, recordParser, cf);
  }

  @Override
  protected void doHandleBodyBuffer(final Buffer buff) {
    if (!hasReadArguments) {
      handleArgs(buff);
    } else {
      handleRow(buff);
    }
  }

  @Override
  protected void doHandleException(final Throwable t) {
    if (!cf.isDone()) {
      cf.completeExceptionally(t);
    } else {
      handleExceptionAfterFutureCompleted(t);
    }
  }

  protected abstract void handleMetadata(QueryResponseMetadata queryResponseMetadata);

  protected abstract void handleRow(Buffer buff);

  protected abstract void handleExceptionAfterFutureCompleted(Throwable t);

  private void handleArgs(final Buffer buff) {
    hasReadArguments = true;

    final QueryResponseMetadata queryResponseMetadata;
    try {
      queryResponseMetadata = ApiJsonMapper.INSTANCE.get()
              .readValue(buff.getBytes(), QueryResponseMetadata.class);
    } catch (Exception e) {
      cf.completeExceptionally(e);
      return;
    }

    handleMetadata(queryResponseMetadata);
  }
}