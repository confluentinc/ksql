/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandFactories implements DdlCommandFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final Map<Class<? extends DdlStatement>, DdlCommandFactory> factories = new HashMap<>();

  public CommandFactories(
      final KafkaTopicClient topicClient,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    factories.put(
        RegisterTopic.class,
        (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new RegisterTopicCommand((RegisterTopic)ddlStatement));
    factories.put(
        CreateStream.class,
        (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new CreateStreamCommand(
            sqlExpression,
            (CreateStream) ddlStatement,
            topicClient,
            enforceTopicExistence
        )
    );
    factories.put(
        CreateTable.class,
        (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new CreateTableCommand(
            sqlExpression,
            (CreateTable) ddlStatement,
            topicClient,
            enforceTopicExistence
        )
    );
    factories.put(
        DropStream.class,
        (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new DropSourceCommand(
            (DropStream) ddlStatement,
            DataSource.DataSourceType.KSTREAM,
            topicClient,
            schemaRegistryClient,
            ((DropStream) ddlStatement).isDeleteTopic()
        )
    );
    factories.put(
        DropTable.class,
        (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new DropSourceCommand(
            (DropTable) ddlStatement,
            DataSource.DataSourceType.KTABLE,
            topicClient,
            schemaRegistryClient,
            ((DropTable) ddlStatement).isDeleteTopic()
        )
    );
    factories.put(
        DropTopic.class, (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new DropTopicCommand(((DropTopic) ddlStatement)));
    factories.put(
        SetProperty.class, (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new SetPropertyCommand(((SetProperty) ddlStatement), properties));
    factories.put(
        UnsetProperty.class, (sqlExpression, ddlStatement, properties, enforceTopicExistence) ->
            new UnsetPropertyCommand(((UnsetProperty) ddlStatement), properties));
  }

  @Override
  public DdlCommand create(
      final String sqlExpression,
      final DdlStatement ddlStatement,
      final Map<String, Object> properties,
      final boolean enforceTopicExistence
  ) {
    final DdlCommandFactory factory = factories.get(ddlStatement.getClass());
    if (factory == null) {
      throw new KsqlException(
          "Unable to find ddl command factory for statement:"
              + ddlStatement.getClass()
              + " valid statements:"
              + factories.keySet()
      );
    }

    return factory.create(sqlExpression, ddlStatement, properties, enforceTopicExistence);
  }
}
