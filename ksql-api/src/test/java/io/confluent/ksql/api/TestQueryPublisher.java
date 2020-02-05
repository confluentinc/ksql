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

package io.confluent.ksql.api;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.BasePublisher;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.utils.RowGenerator;
import io.vertx.core.Context;
import java.util.List;

public class TestQueryPublisher extends BasePublisher<GenericRow> implements QueryPublisher {

  private final RowGenerator rowGenerator;
  private final int rowsBeforePublisherError;
  private final boolean push;
  private int rowsSent;

  public TestQueryPublisher(final Context ctx, final RowGenerator rowGenerator,
      final int rowsBeforePublisherError, final boolean push) {
    super(ctx);
    this.rowGenerator = rowGenerator;
    this.rowsBeforePublisherError = rowsBeforePublisherError;
    this.push = push;
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
          getSubscriber().onNext(row);
        }
      }
    }
  }

  @Override
  public List<String> getColumnNames() {
    return rowGenerator.getColumnNames();
  }

  @Override
  public List<String> getColumnTypes() {
    return rowGenerator.getColumnTypes();
  }

}
