/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMapR2;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Objects;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandFactories implements DdlCommandFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ClassHandlerMapR2<DdlStatement, CommandFactories, CallInfo, DdlCommand>
      FACTORIES = HandlerMaps
      .forClass(DdlStatement.class)
      .withArgTypes(CommandFactories.class, CallInfo.class)
      .withReturnType(DdlCommand.class)
      .put(RegisterTopic.class, CommandFactories::handleRegisterTopic)
      .put(CreateStream.class, CommandFactories::handleCreateStream)
      .put(CreateTable.class, CommandFactories::handleCreateTable)
      .put(DropStream.class, CommandFactories::handleDropStream)
      .put(DropTable.class, CommandFactories::handleDropTable)
      .put(DropTopic.class, CommandFactories::handleDropTopic)
      .build();

  private final ServiceContext serviceContext;

  public CommandFactories(final ServiceContext serviceContext) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
  }

  @Override
  public DdlCommand create(
      final String sqlExpression,
      final DdlStatement ddlStatement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> properties
  ) {
    return FACTORIES
        .getOrDefault(ddlStatement.getClass(), (statement, cf, ci) -> {
          throw new KsqlException(
              "Unable to find ddl command factory for statement:"
                  + statement.getClass()
                  + " valid statements:"
                  + FACTORIES.keySet()
          );
        })
        .handle(
            this,
            new CallInfo(sqlExpression, ksqlConfig, properties),
            ddlStatement);
  }

  private static RegisterTopicCommand handleRegisterTopic(final RegisterTopic statement) {
    return new RegisterTopicCommand(statement);
  }

  private CreateStreamCommand handleCreateStream(
      final CallInfo callInfo,
      final CreateStream statement
  ) {
    return new CreateStreamCommand(
        callInfo.sqlExpression,
        statement,
        callInfo.ksqlConfig,
        serviceContext.getTopicClient());
  }

  private CreateTableCommand handleCreateTable(
      final CallInfo callInfo,
      final CreateTable statement
  ) {
    return new CreateTableCommand(
        callInfo.sqlExpression,
        statement,
        callInfo.ksqlConfig,
        serviceContext.getTopicClient());
  }

  @SuppressWarnings("MethodMayBeStatic")
  private DropSourceCommand handleDropStream(final DropStream statement) {
    return new DropSourceCommand(
        statement,
        DataSourceType.KSTREAM
    );
  }

  @SuppressWarnings("MethodMayBeStatic")
  private DropSourceCommand handleDropTable(final DropTable statement) {
    return new DropSourceCommand(
        statement,
        DataSourceType.KTABLE
    );
  }

  private static DropTopicCommand handleDropTopic(final DropTopic statement) {
    return new DropTopicCommand(statement);
  }

  private static final class CallInfo {

    final String sqlExpression;
    final KsqlConfig ksqlConfig;
    final Map<String, Object> properties;

    private CallInfo(
        final String sqlExpression,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> properties
    ) {
      this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
      this.properties = Objects.requireNonNull(properties, "properties");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig")
          .cloneWithPropertyOverwrite(properties);
    }
  }
}
