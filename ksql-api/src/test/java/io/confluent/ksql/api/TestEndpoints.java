/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.api.TestQueryPublisher.RowGenerator;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

public class TestEndpoints implements Endpoints {

  private final Vertx vertx;
  private Supplier<RowGenerator> rowGeneratorFactory;
  private TestInsertsSubscriber insertsSubscriber;
  private TestAcksPublisher acksPublisher;
  private String lastSql;
  private boolean push;
  private JsonObject lastProperties;
  private String lastTarget;
  private Set<TestQueryPublisher> queryPublishers = new HashSet<>();
  private int acksBeforePublisherError = -1;
  private int rowsBeforePublisherError = -1;

  public TestEndpoints(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public synchronized QueryPublisher createQueryPublisher(final String sql, final boolean push,
      final JsonObject properties) {
    this.lastSql = sql;
    this.push = push;
    this.lastProperties = properties;
    TestQueryPublisher queryPublisher = new TestQueryPublisher(vertx, rowGeneratorFactory.get(),
        rowsBeforePublisherError, push);
    queryPublishers.add(queryPublisher);
    return queryPublisher;
  }

  @Override
  public synchronized InsertsSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<JsonObject> acksSubscriber) {
    this.lastTarget = target;
    this.lastProperties = properties;
    if (acksSubscriber != null) {
      acksPublisher = new TestAcksPublisher(vertx, acksBeforePublisherError);
      acksPublisher.subscribe(acksSubscriber);
      this.insertsSubscriber = new TestInsertsSubscriber(acksPublisher);
    } else {
      this.insertsSubscriber = new TestInsertsSubscriber(null);
    }
    return insertsSubscriber;
  }

  public synchronized void setRowGeneratorFactory(
      final Supplier<RowGenerator> rowGeneratorFactory) {
    this.rowGeneratorFactory = rowGeneratorFactory;
  }

  public synchronized TestInsertsSubscriber getInsertsSubscriber() {
    return insertsSubscriber;
  }

  public synchronized TestAcksPublisher getAcksPublisher() {
    return acksPublisher;
  }

  public synchronized String getLastSql() {
    return lastSql;
  }

  public synchronized JsonObject getLastProperties() {
    return lastProperties;
  }

  public synchronized boolean getLastPush() {
    return push;
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
}

