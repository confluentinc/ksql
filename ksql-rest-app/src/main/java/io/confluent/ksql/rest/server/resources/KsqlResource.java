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
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.server.execution.CustomExecutors;
import io.confluent.ksql.rest.server.execution.DefaultCommandQueueSync;
import io.confluent.ksql.rest.server.execution.RequestHandler;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.rest.server.validation.RequestValidator;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@Path("/ksql")
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class KsqlResource {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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
  private final RequestValidator validator;
  private final RequestHandler handler;


  public KsqlResource(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Function<ServiceContext, SchemaInjector> schemaInjectorFactory
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");

    this.validator = new RequestValidator(
        CustomValidators.VALIDATOR_MAP,
        schemaInjectorFactory,
        ksqlEngine::createSandbox,
        SandboxedServiceContext.create(serviceContext),
        ksqlConfig);
    this.handler = new RequestHandler(
        CustomExecutors.EXECUTOR_MAP,
        new DistributingExecutor(
            commandQueue,
            distributedCmdResponseTimeout,
            schemaInjectorFactory),
        ksqlEngine,
        ksqlConfig,
        serviceContext,
        new DefaultCommandQueueSync(
            commandQueue,
            KsqlResource::shouldSynchronize,
            distributedCmdResponseTimeout)
        );
  }

  @POST
  @Path("/terminate")
  public Response terminateCluster(final ClusterTerminateRequest request) {
    ensureValidPatterns(request.getDeleteTopicList());
    try {
      return Response.ok(
          handler.execute(TERMINATE_CLUSTER, request.getStreamsProperties())
      ).build();
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(
          e, TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT, new KsqlEntityList());
    }
  }

  @POST
  public Response handleKsqlStatements(final KsqlRequest request) {
    if (!ksqlEngine.isAcceptingStatements()) {
      return Errors.serverErrorForStatement(
          new KsqlException("The cluster has been terminated. No new request will be accepted."),
          request.getKsql(),
          new KsqlEntityList()
      );
    }
    activenessRegistrar.updateLastRequestTime();
    try {
      CommandStoreUtil.httpWaitForCommandSequenceNumber(
          commandQueue,
          request,
          distributedCmdResponseTimeout);

      final List<ParsedStatement> statements = ksqlEngine.parse(request.getKsql());
      validator.validate(statements, request.getStreamsProperties(), request.getKsql());

      final KsqlEntityList entities = handler.execute(statements, request.getStreamsProperties());
      return Response.ok(entities).build();
    } catch (final KsqlRestException e) {
      throw e;
    } catch (final KsqlStatementException e) {
      return Errors.badStatement(e.getRawMessage(), e.getSqlStatement());
    } catch (final KsqlException e) {
      return Errors.badRequest(e);
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(e, request.getKsql());
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