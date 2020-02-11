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

package io.confluent.ksql.api.server;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.function.Consumer;
import org.reactivestreams.Subscriber;

public abstract class BaseServerEndpoints implements Endpoints {

  @Override
  public QueryPublisher createQueryPublisher(final String sql, final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor) {
    final BlockingQueryPublisher publisher = createQueryPublisher(context,
        workerExecutor);
    final PushQueryHandler queryHandle = createQuery(sql, properties, context, workerExecutor,
        publisher);
    publisher.setQueryHandle(queryHandle);
    return publisher;
  }

  protected BlockingQueryPublisher createQueryPublisher(final Context context,
      final WorkerExecutor workerExecutor) {
    return new BlockingQueryPublisher(context,
        workerExecutor);
  }

  @Override
  public InsertsSubscriber createInsertsSubscriber(final String target, final JsonObject properties,
      final Subscriber<JsonObject> acksSubscriber) {
    return null;
  }

  protected abstract PushQueryHandler createQuery(String sql, JsonObject properties,
      Context context, WorkerExecutor workerExecutor, Consumer<GenericRow> rowConsumer);
}
