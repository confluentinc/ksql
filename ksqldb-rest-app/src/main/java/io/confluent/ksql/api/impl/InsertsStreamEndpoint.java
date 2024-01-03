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

package io.confluent.ksql.api.impl;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.Identifiers;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.reactivestreams.Subscriber;

public class InsertsStreamEndpoint {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final ReservedInternalTopics reservedInternalTopics;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public InsertsStreamEndpoint(final KsqlEngine ksqlEngine, final KsqlConfig ksqlConfig,
      final ReservedInternalTopics reservedInternalTopics) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.reservedInternalTopics = reservedInternalTopics;
  }

  public InsertsStreamSubscriber createInsertsSubscriber(final String caseInsensitiveTarget,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext) {
    VertxUtils.checkIsWorker();

    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED)) {
      throw new KsqlApiException("The server has disabled INSERT INTO ... VALUES functionality. "
          + "To enable it, restart your ksqlDB server "
          + "with 'ksql.insert.into.values.enabled'=true",
          ERROR_CODE_BAD_REQUEST);
    }

    final String target;
    try {
      target = Identifiers.getIdentifierText(caseInsensitiveTarget);
    } catch (IllegalArgumentException e) {
      throw new KsqlApiException(
          "Invalid target name: " + e.getMessage(), ERROR_CODE_BAD_STATEMENT);
    }


    final DataSource dataSource = getDataSource(ksqlEngine.getMetaStore(),
        SourceName.of(target));
    return InsertsSubscriber.createInsertsSubscriber(serviceContext, properties, dataSource,
        ksqlConfig, context, acksSubscriber, workerExecutor);
  }

  private DataSource getDataSource(
      final MetaStore metaStore,
      final SourceName sourceName
  ) {
    final DataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlApiException(
          "Cannot insert values into an unknown stream/table: " + sourceName
          + metaStore.checkAlternatives(sourceName, Optional.empty()),
            ERROR_CODE_BAD_STATEMENT);
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlApiException(
          "Cannot insert values into windowed stream/table", ERROR_CODE_BAD_STATEMENT);
    }

    if (reservedInternalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlApiException(
          "Cannot insert values into read-only topic: " + dataSource.getKafkaTopicName(),
          ERROR_CODE_BAD_STATEMENT
      );
    }

    return dataSource;
  }

}
