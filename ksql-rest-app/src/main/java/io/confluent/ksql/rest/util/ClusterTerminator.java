/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public class ClusterTerminator {

  @SuppressWarnings("unchecked")
  public void terminateCluster(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final List<String> sourcesList,
      final boolean isKeep,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    ksqlEngine.stopAcceptingStatemens();
    terminateCluster(ksqlEngine, sourcesList, isKeep);
    // Delete the command topic
    deleteCommandTopic(ksqlConfig, ksqlEngine, commandConsumer, commandProducer);
  }

  private void terminateCluster(
      final KsqlEngine ksqlEngine,
      final List<String> sourcesList,
      final boolean isKeep
  ) {
    // Terminate all queries
    getListForSet(ksqlEngine).forEach(
        queryMetadata -> {
          if (queryMetadata instanceof PersistentQueryMetadata) {
            final PersistentQueryMetadata persistentQueryMetadata
                = (PersistentQueryMetadata) queryMetadata;
            ksqlEngine.terminateQuery(persistentQueryMetadata.getQueryId(), true);
          }  else {
            queryMetadata.close();
          }
        }
    );

    // if we have the explicit list of stream/table to delete.
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    if (sourcesList.isEmpty()) {
      metaStore.getAllStructuredDataSources().forEach((s, structuredDataSource) ->
          deleteSource(s, structuredDataSource, ksqlEngine));
    } else {
      if (isKeep) {
        metaStore.getAllStructuredDataSources().forEach(
            (sourceName, structuredDataSource) -> {
              if (!sourcesList.contains(sourceName)) {
                deleteSource(sourceName, structuredDataSource, ksqlEngine);
              }
            }
        );
      } else {
        sourcesList
            .forEach(sourceName -> deleteSource(
                sourceName,
                metaStore.getSource(sourceName),
                ksqlEngine));
      }
    }
  }

  // This is needed because the checkstyle complains if we create this in place.
  private List<QueryMetadata> getListForSet(
      final KsqlEngine ksqlEngine
  ) {
    final List<QueryMetadata> queryMetadataList = new ArrayList<>();
    queryMetadataList.addAll(ksqlEngine.getAllLiveQueries());
    return queryMetadataList;
  }

  private void deleteSource(
      final String sourceName,
      final StructuredDataSource structuredDataSource,
      final KsqlEngine ksqlEngine) {
    final DdlCommand ddlCommand = new DropSourceCommand(
        getAbstractStreamDropStatement(sourceName,
            structuredDataSource.getDataSourceType() == DataSourceType.KSTREAM),
        structuredDataSource.getDataSourceType(),
        ksqlEngine.getTopicClient(),
        ksqlEngine.getSchemaRegistryClient(),
        true
    );
    final DdlCommandExec ddlCommandExec = ksqlEngine.getDdlCommandExec();
    ddlCommandExec.execute(ddlCommand, false);
  }

  private AbstractStreamDropStatement getAbstractStreamDropStatement(
      final String sourceName,
      final boolean isStream) {
    if (isStream) {
      return new DropStream(QualifiedName.of(sourceName), false, true);
    } else {
      return new DropTable(QualifiedName.of(sourceName), false, true);
    }
  }

  private void deleteCommandTopic(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer) {
    // Delete the command topic
    commandConsumer.close();
    commandProducer.close();
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    final KafkaTopicClient kafkaTopicClient = ksqlEngine.getTopicClient();
    try {
      ExecutorUtil.executeWithRetries(
          () -> kafkaTopicClient.deleteTopics(
              Collections.singletonList(commandTopic)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException("Could not delete the command topic: "
          + commandTopic, e);
    }
  }

}
