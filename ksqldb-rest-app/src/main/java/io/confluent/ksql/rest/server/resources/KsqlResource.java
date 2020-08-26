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

package io.confluent.ksql.rest.server.resources;

import static java.util.regex.Pattern.compile;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.server.computation.ValidatedCommandFactory;
import io.confluent.ksql.rest.server.execution.CustomExecutors;
import io.confluent.ksql.rest.server.execution.DefaultCommandQueueSync;
import io.confluent.ksql.rest.server.execution.RequestHandler;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.rest.server.validation.RequestValidator;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlResource implements KsqlConfigurable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(KsqlResource.class);

  private static final List<ParsedStatement> TERMINATE_CLUSTER =
      new DefaultKsqlParser().parse(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);

  private static final Set<Class<? extends Statement>> SYNC_BLACKLIST =
      ImmutableSet.<Class<? extends Statement>>builder()
          .add(ListTopics.class)
          .add(ListFunctions.class)
          .add(DescribeFunction.class)
          .add(ListProperties.class)
          .add(SetProperty.class)
          .add(UnsetProperty.class)
          .build();

  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final Duration distributedCmdResponseTimeout;
  private final ActivenessRegistrar activenessRegistrar;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final DenyListPropertyValidator denyListPropertyValidator;
  private RequestValidator validator;
  private RequestHandler handler;
  private final Errors errorHandler;
  private KsqlHostInfo localHost;
  private URL localUrl;

  public KsqlResource(
      final KsqlEngine ksqlEngine,
      final CommandRunner commandRunner,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final DenyListPropertyValidator denyListPropertyValidator
  ) {
    this(
        ksqlEngine,
        commandRunner,
        distributedCmdResponseTimeout,
        activenessRegistrar,
        Injectors.DEFAULT,
        authorizationValidator,
        errorHandler,
        denyListPropertyValidator
    );
  }

  KsqlResource(
      final KsqlEngine ksqlEngine,
      final CommandRunner commandRunner,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final DenyListPropertyValidator denyListPropertyValidator
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
    this.authorizationValidator = Objects
        .requireNonNull(authorizationValidator, "authorizationValidator");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.denyListPropertyValidator =
        Objects.requireNonNull(denyListPropertyValidator, "denyListPropertyValidator");
  }

  @Override
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    final String applicationServer =
        (String) config.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG);
    final HostInfo hostInfo = ServerUtil.parseHostInfo(applicationServer);
    this.localHost = new KsqlHostInfo(hostInfo.host(), hostInfo.port());
    try {
      this.localUrl = new URL(applicationServer);
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to convert remote host info to URL."
          + " remoteInfo: " + localHost.host() + ":"
          + localHost.host());
    }

    this.validator = new RequestValidator(
        CustomValidators.VALIDATOR_MAP,
        injectorFactory,
        ksqlEngine::createSandbox,
        config,
        new ValidatedCommandFactory()
    );

    this.handler = new RequestHandler(
        CustomExecutors.EXECUTOR_MAP,
        new DistributingExecutor(
            config,
            commandRunner.getCommandQueue(),
            distributedCmdResponseTimeout,
            injectorFactory,
            authorizationValidator,
            new ValidatedCommandFactory(),
            errorHandler,
            () -> commandRunner.checkCommandRunnerStatus()
                == CommandRunner.CommandRunnerStatus.DEGRADED
        ),
        ksqlEngine,
        config,
        new DefaultCommandQueueSync(
            commandRunner.getCommandQueue(),
            KsqlResource::shouldSynchronize,
            distributedCmdResponseTimeout
        )
    );
  }

  public EndpointResponse terminateCluster(
      final KsqlSecurityContext securityContext,
      final ClusterTerminateRequest request
  ) {
    LOG.info("Received: " + request);

    throwIfNotConfigured();

    ensureValidPatterns(request.getDeleteTopicList());
    try {
      final Map<String, Object> streamsProperties = request.getStreamsProperties();
      denyListPropertyValidator.validateAll(streamsProperties);

      final KsqlEntityList entities = handler.execute(
          securityContext,
          TERMINATE_CLUSTER,
          new SessionProperties(
              streamsProperties,
              localHost,
              localUrl,
              false
          )
      );
      return EndpointResponse.ok(entities);
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(
          e, TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT, new KsqlEntityList());
    }
  }

  public EndpointResponse handleKsqlStatements(
      final KsqlSecurityContext securityContext,
      final KsqlRequest request
  ) {
    LOG.info("Received: " + request);

    throwIfNotConfigured();

    activenessRegistrar.updateLastRequestTime();

    try {
      CommandStoreUtil.httpWaitForCommandSequenceNumber(
          commandRunner.getCommandQueue(),
          request,
          distributedCmdResponseTimeout);

      final Map<String, Object> configProperties = request.getConfigOverrides();
      denyListPropertyValidator.validateAll(configProperties);

      final KsqlRequestConfig requestConfig =
          new KsqlRequestConfig(request.getRequestProperties());
      final List<ParsedStatement> statements = ksqlEngine.parse(request.getKsql());

      validator.validate(
          SandboxedServiceContext.create(securityContext.getServiceContext()),
          statements,
          new SessionProperties(
              configProperties,
              localHost,
              localUrl,
              requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST)
          ),
          request.getKsql()
      );

      final KsqlEntityList entities = handler.execute(
          securityContext,
          statements,
          new SessionProperties(
              configProperties,
              localHost,
              localUrl,
              requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST)
          )
      );

      LOG.info("Processed successfully: " + request);
      addCommandRunnerDegradedWarning(
          entities,
          errorHandler,
          () -> commandRunner.checkCommandRunnerStatus()
              == CommandRunner.CommandRunnerStatus.DEGRADED);
      return EndpointResponse.ok(entities);
    } catch (final KsqlRestException e) {
      LOG.info("Processed unsuccessfully: " + request + ", reason: " + e.getMessage());
      throw e;
    } catch (final KsqlStatementException e) {
      LOG.info("Processed unsuccessfully: " + request + ", reason: " + e.getMessage());
      return Errors.badStatement(e.getRawMessage(), e.getSqlStatement());
    } catch (final KsqlException e) {
      LOG.info("Processed unsuccessfully: " + request + ", reason: " + e.getMessage());
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    } catch (final Exception e) {
      LOG.info("Processed unsuccessfully: " + request + ", reason: " + e.getMessage());
      return errorHandler.generateResponse(
          e, Errors.serverErrorForStatement(e, request.getKsql()));
    }
  }

  private void throwIfNotConfigured() {
    if (validator == null || handler == null) {
      throw new KsqlRestException(Errors.notReady());
    }
  }

  private static boolean shouldSynchronize(final Class<? extends Statement> statementClass) {
    return !SYNC_BLACKLIST.contains(statementClass)
        // we never need to synchronize distributed statements
        && CustomExecutors.EXECUTOR_MAP.containsKey(statementClass);
  }

  private static void ensureValidPatterns(final List<String> deleteTopicList) {
    deleteTopicList
        .forEach(pattern -> {
          try {
            compile(pattern);
          } catch (final PatternSyntaxException patternSyntaxException) {
            throw new KsqlRestException(Errors.badRequest("Invalid pattern: " + pattern));
          }
        });
  }

  private static void addCommandRunnerDegradedWarning(
      final KsqlEntityList entityList,
      final Errors errorHandler,
      final Supplier<Boolean> commandRunnerDegraded
  ) {
    if (commandRunnerDegraded.get()) {
      for (final KsqlEntity entity: entityList) {
        entity.updateWarnings(Collections.singletonList(
            new KsqlWarning(errorHandler.commandRunnerDegradedErrorMessage())));
      }
    }
  }
}
