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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMapR2;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandFactories implements DdlCommandFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ClassHandlerMapR2<DdlStatement, CommandFactories, CallInfo, DdlCommand>
      FACTORIES = HandlerMaps
      .forClass(DdlStatement.class)
      .withArgTypes(CommandFactories.class, CallInfo.class)
      .withReturnType(DdlCommand.class)
      .put(CreateStream.class, CommandFactories::handleCreateStream)
      .put(CreateTable.class, CommandFactories::handleCreateTable)
      .put(DropStream.class, CommandFactories::handleDropStream)
      .put(DropTable.class, CommandFactories::handleDropTable)
      .put(RegisterType.class, CommandFactories::handleRegisterType)
      .put(DropType.class, CommandFactories::handleDropType)
      .put(AlterSource.class, CommandFactories::handleAlterSource)
      .build();

  private final CreateSourceFactory createSourceFactory;
  private final DropSourceFactory dropSourceFactory;
  private final RegisterTypeFactory registerTypeFactory;
  private final DropTypeFactory dropTypeFactory;
  private final AlterSourceFactory alterSourceFactory;

  public CommandFactories(final ServiceContext serviceContext, final MetaStore metaStore) {
    this(
        new CreateSourceFactory(serviceContext, metaStore),
        new DropSourceFactory(metaStore),
        new RegisterTypeFactory(metaStore),
        new DropTypeFactory(metaStore),
        new AlterSourceFactory(metaStore)
    );
  }

  @VisibleForTesting
  CommandFactories(
      final CreateSourceFactory createSourceFactory,
      final DropSourceFactory dropSourceFactory,
      final RegisterTypeFactory registerTypeFactory,
      final DropTypeFactory dropTypeFactory,
      final AlterSourceFactory alterSourceFactory
  ) {
    this.createSourceFactory =
        Objects.requireNonNull(createSourceFactory, "createSourceFactory");
    this.dropSourceFactory = Objects.requireNonNull(dropSourceFactory, "dropSourceFactory");
    this.registerTypeFactory =
        Objects.requireNonNull(registerTypeFactory, "registerTypeFactory");
    this.dropTypeFactory = Objects.requireNonNull(dropTypeFactory, "dropTypeFactory");
    this.alterSourceFactory = Objects.requireNonNull(alterSourceFactory, "alterSourceFactory");
  }

  @Override
  public DdlCommand create(
      final String sqlExpression,
      final DdlStatement ddlStatement,
      final SessionConfig config
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
            new CallInfo(sqlExpression, config),
            ddlStatement);
  }

  @Override
  public DdlCommand create(
      final KsqlStructuredDataOutputNode outputNode,
      final Optional<RefinementInfo> emitStrategy
  ) {
    if (outputNode.getNodeOutputType() == DataSource.DataSourceType.KSTREAM) {
      return createSourceFactory.createStreamCommand(outputNode);
    } else {
      return createSourceFactory.createTableCommand(outputNode, emitStrategy);
    }
  }

  private CreateStreamCommand handleCreateStream(
      final CallInfo callInfo,
      final CreateStream statement
  ) {
    return createSourceFactory.createStreamCommand(
        statement,
        callInfo.config.getConfig(true)
    );
  }

  private CreateTableCommand handleCreateTable(
      final CallInfo callInfo,
      final CreateTable statement
  ) {
    return createSourceFactory.createTableCommand(
        statement,
        callInfo.config.getConfig(true)
    );
  }

  private DropSourceCommand handleDropStream(final DropStream statement) {
    return dropSourceFactory.create(statement);
  }

  private DropSourceCommand handleDropTable(final DropTable statement) {
    return dropSourceFactory.create(statement);
  }

  private RegisterTypeCommand handleRegisterType(final RegisterType statement) {
    return registerTypeFactory.create(statement);
  }

  private DropTypeCommand handleDropType(final DropType statement) {
    return dropTypeFactory.create(statement);
  }

  private AlterSourceCommand handleAlterSource(final AlterSource statement) {
    return alterSourceFactory.create(statement);
  }

  private static final class CallInfo {

    final String sqlExpression;
    final SessionConfig config;

    private CallInfo(
        final String sqlExpression,
        final SessionConfig config
    ) {
      this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
      this.config = Objects.requireNonNull(config, "config");
    }
  }
}
