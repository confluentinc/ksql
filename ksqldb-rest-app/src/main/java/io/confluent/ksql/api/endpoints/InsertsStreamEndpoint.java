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

package io.confluent.ksql.api.endpoints;

import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Subscriber;

public class InsertsStreamEndpoint {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final ReservedInternalTopics reservedInternalTopics;

  public InsertsStreamEndpoint(final KsqlEngine ksqlEngine, final KsqlConfig ksqlConfig,
      final ReservedInternalTopics reservedInternalTopics) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.reservedInternalTopics = reservedInternalTopics;
  }

  public InsertsStreamSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext) {
    VertxUtils.checkIsWorker();
    final DataSource dataSource = getDataSource(ksqlEngine.getMetaStore(),
        SourceName.of(target));
    if (dataSource.getDataSourceType() == DataSourceType.KTABLE) {
      throw new KsqlException("Cannot insert into a table");
    }
    return InsertsSubscriber.createInsertsSubscriber(serviceContext, properties, dataSource,
        ksqlConfig, context, acksSubscriber, workerExecutor);
  }

  private DataSource getDataSource(
      final MetaStore metaStore,
      final SourceName sourceName
  ) {
    final DataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream: "
          + sourceName);
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlException("Cannot insert values into windowed stream");
    }

    if (reservedInternalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlException("Cannot insert values into read-only topic: "
          + dataSource.getKafkaTopicName());
    }

    return dataSource;
  }

}
