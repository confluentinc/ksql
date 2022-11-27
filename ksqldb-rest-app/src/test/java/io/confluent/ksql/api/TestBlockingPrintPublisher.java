/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.utils.RowGenerator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BasePublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.vertx.core.Context;

import java.util.List;
import java.util.Optional;

public class TestBlockingPrintPublisher
    extends BasePublisher<String> {

  private final RowGenerator rowGenerator;
  private final int rowsBeforePublisherError;
  private final boolean push;
  private final int limit;
  private final QueryId queryId;
  private int rowsSent;

  public TestBlockingPrintPublisher(final Context ctx, final RowGenerator rowGenerator,
      final int rowsBeforePublisherError, final boolean push, final int limit,
      final QueryId queryId) {
    super(ctx);
    this.rowGenerator = rowGenerator;
    this.rowsBeforePublisherError = rowsBeforePublisherError;
    this.push = push;
    this.limit = limit;
    this.queryId = queryId;
  }

  synchronized boolean hasSubscriber() {
    return getSubscriber() != null;
  }

  @Override
  protected void maybeSend() {
    doSend(getDemand());
  }

  private void doSend(long amount) {
    if (getSubscriber() == null) {
      return;
    }
    for (int i = 0; i < amount; i++) {
      GenericRow row = rowGenerator.getNext();
      if (row == null) {
        if (!push) {
          sendComplete();
        }
        break;
      } else {
        if (rowsBeforePublisherError != -1 && rowsSent == rowsBeforePublisherError) {
          // Inject an error
          getSubscriber().onError(new RuntimeException("Failure in processing"));
        } else {
          rowsSent++;
          getSubscriber().onNext(
              "rowtime: 2022/11/27 13:59:37.552 Z, key: {\"F1\":[\"a\"]}, value: {\"STR\":\"FOO\",\"LONG\":1,\"DEC\":1.11,\"BYTES_\":\"AQ==\",\"ARRAY\":[\"a\"],\"MAP\":{\"k1\":\"v1\"},\"STRUCT\":{\"F1\":2},\"COMPLEX\":{\"DECIMAL\":0.0,\"STRUCT\":{\"F1\":\"v0\",\"F2\":0},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v0\"}],\"ARRAY_MAP\":[{\"k1\":0}],\"MAP_ARRAY\":{\"k\":[\"v0\"]},\"MAP_MAP\":{\"k\":{\"k\":0}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v0\"}}},\"TIMESTAMP\":1,\"DATE\":1,\"TIME\":0}, partition: 0");
          if (rowsSent == limit) {
            sendComplete();
          }
        }
      }
    }
  }

  public void sendError() {
    if (rowsBeforePublisherError != -1) {
      throw new IllegalStateException("Cannot send error if rowsBeforePublisherError is set");
    }
    getSubscriber().onError(new RuntimeException("Failure in processing"));
  }

}
