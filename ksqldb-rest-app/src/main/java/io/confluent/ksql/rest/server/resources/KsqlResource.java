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
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.computation.CommandQueue;
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
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.PatternSyntaxException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@Path("/ksql")
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
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
  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final ActivenessRegistrar activenessRegistrar;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private RequestValidator validator;
  private RequestHandler handler;
  private final Errors errorHandler;


  public KsqlResource(
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler
  ) {
    this(
        ksqlEngine,
        commandQueue,
        distributedCmdResponseTimeout,
        activenessRegistrar,
        Injectors.DEFAULT,
        authorizationValidator,
        errorHandler
    );
  }

  KsqlResource(
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
    this.authorizationValidator = Objects
        .requireNonNull(authorizationValidator, "authorizationValidator");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
  }

  @Override
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    this.validator = new RequestValidator(
        CustomValidators.VALIDATOR_MAP,
        injectorFactory,
        ksqlEngine::createSandbox,
        config,
        new ValidatedCommandFactory(config)
    );

    this.handler = new RequestHandler(
        CustomExecutors.EXECUTOR_MAP,
        new DistributingExecutor(
            config,
            commandQueue,
            distributedCmdResponseTimeout,
            injectorFactory,
            authorizationValidator,
            new ValidatedCommandFactory(config),
            errorHandler
        ),
        ksqlEngine,
        config,
        new DefaultCommandQueueSync(
            commandQueue,
            KsqlResource::shouldSynchronize,
            distributedCmdResponseTimeout
        )
    );
  }

  @POST
  @Path("/terminate")
  public Response terminateCluster(
      @Context final KsqlSecurityContext securityContext,
      final ClusterTerminateRequest request
  ) {
    LOG.info("Received: " + request);

    throwIfNotConfigured();

    ensureValidPatterns(request.getDeleteTopicList());
    try {
      final KsqlEntityList entities = handler.execute(
          securityContext,
          TERMINATE_CLUSTER,
          request.getStreamsProperties()
      );
      return Response.ok(entities).build();
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(
          e, TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT, new KsqlEntityList());
    }
  }

  @POST
  public Response handleKsqlStatements(
      @Context final KsqlSecurityContext securityContext,
      final KsqlRequest request
  ) {
    ApiServerUtils.setMaskedSqlIfNeeded(request);
    QueryLogger.info("Received: " + request.toStringWithoutQuery(), request.getMaskedKsql());
    throwIfNotConfigured();

    activenessRegistrar.updateLastRequestTime();

    try {
      CommandStoreUtil.httpWaitForCommandSequenceNumber(
          commandQueue,
          request,
          distributedCmdResponseTimeout);

      final List<ParsedStatement> statements = ksqlEngine.parse(request.getUnmaskedKsql());
      validator.validate(
          SandboxedServiceContext.create(securityContext.getServiceContext()),
          statements,
          request.getStreamsProperties(),
          request.getUnmaskedKsql()
      );

      final KsqlEntityList entities = handler.execute(
          securityContext,
          statements,
          request.getStreamsProperties()
      );

      QueryLogger.info(
          "Processed successfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql()
      );
      return Response.ok(entities).build();
    } catch (final KsqlRestException e) {
      QueryLogger.info(
          "Processed unsuccessfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql(),
          e
      );
      throw e;
    } catch (final KsqlStatementException e) {
      QueryLogger.info(
          "Processed unsuccessfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql(),
          e
      );
      final Response response;
      if (e.getProblem() == KsqlStatementException.Problem.STATEMENT) {
        response = Errors.badStatement(e.getRawUnloggedDetails(), e.getSqlStatement());
      } else if (e.getProblem() == KsqlStatementException.Problem.OTHER) {
        response = Errors.serverErrorForStatement(e, e.getSqlStatement());
      } else {
        response = Errors.badRequest(e);
      }
      return errorHandler.generateResponse(e, response);
    } catch (final KsqlException e) {
      QueryLogger.info(
          "Processed unsuccessfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql(),
          e
      );
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    } catch (final Exception e) {
      QueryLogger.info(
          "Processed unsuccessfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql(),
          e
      );
      return errorHandler.generateResponse(
          e,
          Errors.serverErrorForStatement(e, request.getMaskedKsql())
      );
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
}
