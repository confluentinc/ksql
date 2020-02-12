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

import io.confluent.ksql.api.server.BufferedPublisher;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.utils.RowGenerator;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

public class TestEndpoints implements Endpoints {

  private Supplier<RowGenerator> rowGeneratorFactory;
  private TestInsertsSubscriber insertsSubscriber;
  private String lastSql;
  private JsonObject lastProperties;
  private String lastTarget;
  private Set<TestQueryPublisher> queryPublishers = new HashSet<>();
  private int acksBeforePublisherError = -1;
  private int rowsBeforePublisherError = -1;
  private RuntimeException createQueryPublisherException;

  @Override
  public synchronized QueryPublisher createQueryPublisher(final String sql,
      final JsonObject properties, final Context context, final WorkerExecutor workerExecutor) {
    if (createQueryPublisherException != null) {
      createQueryPublisherException.fillInStackTrace();
      throw createQueryPublisherException;
    }
    this.lastSql = sql;
    this.lastProperties = properties;
    boolean push = sql.toLowerCase().contains("emit changes");
    TestQueryPublisher queryPublisher = new TestQueryPublisher(context,
        rowGeneratorFactory.get(),
        rowsBeforePublisherError,
        push);
    queryPublishers.add(queryPublisher);
    return queryPublisher;
  }

  @Override
  public synchronized InsertsStreamSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber,
      final Context context,
      final WorkerExecutor workerExecutor) {
    this.lastTarget = target;
    this.lastProperties = properties;
    BufferedPublisher<InsertResult> acksPublisher = new BufferedPublisher<>(Vertx.currentContext());
    acksPublisher.subscribe(acksSubscriber);
    this.insertsSubscriber = new TestInsertsSubscriber(Vertx.currentContext(), acksPublisher,
        acksBeforePublisherError);
    return insertsSubscriber;
  }

  public synchronized void setRowGeneratorFactory(
      final Supplier<RowGenerator> rowGeneratorFactory) {
    this.rowGeneratorFactory = rowGeneratorFactory;
  }

  public synchronized TestInsertsSubscriber getInsertsSubscriber() {
    return insertsSubscriber;
  }

  public synchronized String getLastSql() {
    return lastSql;
  }

  public synchronized JsonObject getLastProperties() {
    return lastProperties;
  }

  public synchronized Set<TestQueryPublisher> getQueryPublishers() {
    return queryPublishers;
  }

  public synchronized String getLastTarget() {
    return lastTarget;
  }

  public synchronized void setAcksBeforePublisherError(final int acksBeforePublisherError) {
    this.acksBeforePublisherError = acksBeforePublisherError;
  }

  public synchronized void setRowsBeforePublisherError(final int rowsBeforePublisherError) {
    this.rowsBeforePublisherError = rowsBeforePublisherError;
  }

  public synchronized void setCreateQueryPublisherException(final RuntimeException exception) {
    this.createQueryPublisherException = exception;
  }
}

