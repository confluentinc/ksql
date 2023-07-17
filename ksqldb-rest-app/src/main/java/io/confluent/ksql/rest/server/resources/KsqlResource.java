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
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
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
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlTestResult;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.PropertiesList;
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
import io.confluent.ksql.tools.test.SqlTestExecutor;
import io.confluent.ksql.tools.test.parser.SqlTestLoader;
import io.confluent.ksql.tools.test.parser.SqlTestLoader.SqlTest;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConfigurable;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.io.FileUtils;
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

  private final KsqlExecutionContext ksqlEngine;
  private final CommandRunner commandRunner;
  private final Duration distributedCmdResponseTimeout;
  private final ActivenessRegistrar activenessRegistrar;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final DenyListPropertyValidator denyListPropertyValidator;
  private final Supplier<String> commandRunnerWarning;
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
        denyListPropertyValidator,
        commandRunner::getCommandRunnerDegradedWarning
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
      final DenyListPropertyValidator denyListPropertyValidator,
      final Supplier<String> commandRunnerWarning
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
    this.commandRunnerWarning =
        Objects.requireNonNull(commandRunnerWarning, "commandRunnerWarning");
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
            commandRunnerWarning
        ),
        ksqlEngine,
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

  public EndpointResponse isValidProperty(final String property) {
    try {
      final Map<String, Object> properties = new HashMap<>();
      properties.put(property, "");
      denyListPropertyValidator.validateAll(properties);
      final KsqlConfigResolver resolver = new KsqlConfigResolver();
      final Optional<ConfigItem> resolvedItem = resolver.resolve(property, false);
      if (ksqlEngine.getKsqlConfig().getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)
          && resolvedItem.isPresent()) {
        if (!PropertiesList.QueryLevelProperties.contains(resolvedItem.get().getPropertyName())) {
          throw new KsqlException(String.format("When shared runtimes are enabled, the"
              + " config %s can only be set for the entire cluster and all queries currently"
              + " running in it, and not configurable for individual queries."
              + " Please use ALTER SYSTEM to change this config for all queries.",
              properties));
        }
      }
      return EndpointResponse.ok(true);
    } catch (final KsqlException e) {
      LOG.info("Processed unsuccessfully, reason: ", e);
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    } catch (final Exception e) {
      LOG.info("Processed unsuccessfully, reason: ", e);
      throw e;
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  // CHECKSTYLE_RULES.OFF: JavaNCSS
  public EndpointResponse handleKsqlStatements(
      final KsqlSecurityContext securityContext,
      final KsqlRequest request
  ) {
    // CHECKSTYLE_RULES.ON: JavaNCSS
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // Set masked sql statement if request is not from OldApiUtils.handleOldApiRequest
    ApiServerUtils.setMaskedSqlIfNeeded(request);

    QueryLogger.info("Received: " + request.toStringWithoutQuery(), request.getMaskedKsql());
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
      final List<ParsedStatement> statements = ksqlEngine.parse(request.getUnmaskedKsql());

      validator.validate(
          SandboxedServiceContext.create(securityContext.getServiceContext()),
          statements,
          new SessionProperties(
              configProperties,
              localHost,
              localUrl,
              requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST),
              request.getSessionVariables()
          ),
          request.getUnmaskedKsql()
      );

      // log validated statements for query anonymization
      statements.forEach(s -> {
        if (s.getUnMaskedStatementText().toLowerCase().contains("terminate")
            || s.getUnMaskedStatementText().toLowerCase().contains("drop")) {
          QueryLogger.info("Query terminated", s.getMaskedStatementText());
        } else {
          QueryLogger.info("Query created", s.getMaskedStatementText());
        }
      });

      final KsqlEntityList entities = handler.execute(
          securityContext,
          statements,
          new SessionProperties(
              configProperties,
              localHost,
              localUrl,
              requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST),
              request.getSessionVariables()
          )
      );

      QueryLogger.info(
          "Processed successfully: " + request.toStringWithoutQuery(),
          request.getMaskedKsql()
      );
      addCommandRunnerWarning(
          entities,
          commandRunnerWarning);
      return EndpointResponse.ok(entities);
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
      final EndpointResponse response;
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

  public EndpointResponse runTest(final String test) {
    try {
      final List<SqlTest> tests = SqlTestLoader.loadTest(test);
      return EndpointResponse.ok(runTests(tests));
    } catch (final Exception e) {
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    }
  }

  private List<KsqlTestResult> runTests(final List<SqlTest> tests) throws IOException {
    final List<KsqlTestResult> results = new ArrayList<>();
    for (final SqlTest test : tests) {
      final Path tempFolder = Files.createTempDirectory("test-temp");
      final SqlTestExecutor executor = SqlTestExecutor.create(tempFolder);

      try {
        executor.executeTest(test);
        results.add(new KsqlTestResult(true, test.getName(), ""));
      } catch (final Throwable e) {
        results.add(new KsqlTestResult(false, test.getName(), e.getMessage()));
      } finally {
        cleanUp(executor, tempFolder);
        executor.close();
      }
    }
    return results;
  }

  private static void cleanUp(final SqlTestExecutor executor, final Path tempFolder) {
    executor.close();
    try {
      FileUtils.deleteDirectory(tempFolder.toFile());
    } catch (final Exception e) {
      LOG.warn("Failed to clean up temporary test folder: " + e.getMessage());
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

  private static void addCommandRunnerWarning(
      final KsqlEntityList entityList,
      final Supplier<String> commandRunnerWarning
  ) {
    final String commandRunnerIssueString = commandRunnerWarning.get();
    if (!commandRunnerIssueString.equals("")) {
      for (final KsqlEntity entity: entityList) {
        entity.updateWarnings(Collections.singletonList(
            new KsqlWarning(commandRunnerIssueString)));
      }
    }
  }
}
