package io.confluent.ksql;

import io.confluent.ksql.api.ApiTest;
import io.confluent.ksql.api.AuthTest;
import io.confluent.ksql.api.BaseApiTest;
import io.confluent.ksql.api.CorsTest;
import io.confluent.ksql.api.Http2OnlyStreamTest;
import io.confluent.ksql.api.ListenersTest;
import io.confluent.ksql.api.MaxQueriesTest;
import io.confluent.ksql.api.ServerCorsTest;
import io.confluent.ksql.api.TlsTest;
import io.confluent.ksql.logging.processing.ProcessingLogServerUtilsTest;
import io.confluent.ksql.rest.app.ImmutabilityTest;
import io.confluent.ksql.rest.entity.QueryDescriptionFactoryTest;
import io.confluent.ksql.rest.entity.SourceDescriptionFactoryTest;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgentTest;
import io.confluent.ksql.rest.healthcheck.HealthCheckResponseTest;
import io.confluent.ksql.rest.integration.AuthorizationFunctionalTest;
import io.confluent.ksql.rest.integration.BackupRollbackIntegrationTest;
import io.confluent.ksql.rest.integration.ClusterTerminationTest;
import io.confluent.ksql.rest.integration.CommandTopicFunctionalTest;
import io.confluent.ksql.rest.integration.HealthCheckResourceFunctionalTest;
import io.confluent.ksql.rest.integration.HeartbeatAgentFunctionalTest;
import io.confluent.ksql.rest.integration.KsqlResourceFunctionalTest;
import io.confluent.ksql.rest.integration.LagReportingAgentFunctionalTest;
import io.confluent.ksql.rest.integration.PauseResumeIntegrationTest;
import io.confluent.ksql.rest.integration.PreconditionCheckerIntegrationTest;
import io.confluent.ksql.rest.integration.ProcessingLogErrorMetricFunctionalTest;
import io.confluent.ksql.rest.integration.PullQueryFunctionalTest;
import io.confluent.ksql.rest.integration.PullQueryLimitHARoutingTest;
import io.confluent.ksql.rest.integration.PullQueryMetricsFunctionalTest;
import io.confluent.ksql.rest.integration.PullQueryMetricsHttp2FunctionalTest;
import io.confluent.ksql.rest.integration.PullQueryMetricsWSFunctionalTest;
import io.confluent.ksql.rest.integration.PullQueryRoutingFunctionalTest;
import io.confluent.ksql.rest.integration.PullQuerySingleNodeFunctionalTest;
import io.confluent.ksql.rest.integration.QueryRestartMetricFunctionalTest;
import io.confluent.ksql.rest.integration.RestApiTest;
import io.confluent.ksql.rest.integration.ScalablePushQueryFunctionalTest;
import io.confluent.ksql.rest.integration.ShowQueriesMultiNodeFunctionalTest;
import io.confluent.ksql.rest.integration.ShowQueriesMultiNodeWithTlsFunctionalTest;
import io.confluent.ksql.rest.integration.SystemAuthenticationFunctionalTest;
import io.confluent.ksql.rest.integration.TerminateTransientQueryFunctionalTest;
import io.confluent.ksql.rest.integration.TransientQueryResourceCleanerIntTest;
import io.confluent.ksql.rest.server.ActiveHostFilterTest;
import io.confluent.ksql.rest.server.BackupReplayFileTest;
import io.confluent.ksql.rest.server.CommandTopicBackupImplTest;
import io.confluent.ksql.rest.server.CommandTopicTest;
import io.confluent.ksql.rest.server.ConnectIntegrationTest;
import io.confluent.ksql.rest.server.HeartbeatAgentTest;
import io.confluent.ksql.rest.server.InsertionIntegrationTest;
import io.confluent.ksql.rest.server.KsqlPlanSchemaTest;
import io.confluent.ksql.rest.server.KsqlRestApplicationTest;
import io.confluent.ksql.rest.server.KsqlRestConfigTest;
import io.confluent.ksql.rest.server.KsqlServerMainTest;
import io.confluent.ksql.rest.server.LagReportingAgentTest;
import io.confluent.ksql.rest.server.LivenessFilterTest;
import io.confluent.ksql.rest.server.LocalCommandsFileTest;
import io.confluent.ksql.rest.server.LocalCommandsTest;
import io.confluent.ksql.rest.server.MaximumLagFilterTest;
import io.confluent.ksql.rest.server.MultiExecutableTest;
import io.confluent.ksql.rest.server.PreconditionCheckerTest;
import io.confluent.ksql.rest.server.ServerOptionsTest;
import io.confluent.ksql.rest.server.ServerUtilTest;
import io.confluent.ksql.rest.server.StandaloneExecutorFactoryTest;
import io.confluent.ksql.rest.server.StandaloneExecutorFunctionalTest;
import io.confluent.ksql.rest.server.StandaloneExecutorTest;
import io.confluent.ksql.rest.server.TestKsqlRestAppTest;
import io.confluent.ksql.rest.server.computation.CommandRunnerMetricsTest;
import io.confluent.ksql.rest.server.computation.CommandRunnerTest;
import io.confluent.ksql.rest.server.computation.CommandStoreTest;
import io.confluent.ksql.rest.server.computation.CommandTest;
import io.confluent.ksql.rest.server.computation.ConfigTopicKeyTest;
import io.confluent.ksql.rest.server.computation.DeprecatedStatementsCheckerTest;
import io.confluent.ksql.rest.server.computation.DistributingExecutorTest;
import io.confluent.ksql.rest.server.computation.InteractiveStatementExecutorTest;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdesTest;
import io.confluent.ksql.rest.server.computation.KafkaConfigStoreTest;
import io.confluent.ksql.rest.server.computation.RecoveryTest;
import io.confluent.ksql.rest.server.computation.SequenceNumberFutureStoreTest;
import io.confluent.ksql.rest.server.computation.ValidatedCommandFactoryTest;
import io.confluent.ksql.rest.server.execution.AssertSchemaExecutorTest;
import io.confluent.ksql.rest.server.execution.AssertTopicExecutorTest;
import io.confluent.ksql.rest.server.execution.ConnectExecutorTest;
import io.confluent.ksql.rest.server.execution.DefaultCommandQueueSyncTest;
import io.confluent.ksql.rest.server.execution.DescribeConnectorExecutorTest;
import io.confluent.ksql.rest.server.execution.DescribeFunctionExecutorTest;
import io.confluent.ksql.rest.server.execution.DropConnectorExecutorTest;
import io.confluent.ksql.rest.server.execution.ExplainExecutorTest;
import io.confluent.ksql.rest.server.execution.InsertValuesExecutorTest;
import io.confluent.ksql.rest.server.execution.ListConnectorPluginsTest;
import io.confluent.ksql.rest.server.execution.ListConnectorsExecutorTest;
import io.confluent.ksql.rest.server.execution.ListFunctionsExecutorTest;
import io.confluent.ksql.rest.server.execution.ListPropertiesExecutorTest;
import io.confluent.ksql.rest.server.execution.ListQueriesExecutorTest;
import io.confluent.ksql.rest.server.execution.ListSourceExecutorTest;
import io.confluent.ksql.rest.server.execution.ListTopicsExecutorTest;
import io.confluent.ksql.rest.server.execution.ListTypesExecutorTest;
import io.confluent.ksql.rest.server.execution.ListVariablesExecutorTest;
import io.confluent.ksql.rest.server.execution.PropertyExecutorTest;
import io.confluent.ksql.rest.server.execution.PullQueryMetricsTest;
import io.confluent.ksql.rest.server.execution.RemoteHostExecutorTest;
import io.confluent.ksql.rest.server.execution.RemoteSourceDescriptionExecutorTest;
import io.confluent.ksql.rest.server.execution.RequestHandlerTest;
import io.confluent.ksql.rest.server.execution.ScalablePushQueryMetricsTest;
import io.confluent.ksql.rest.server.execution.TerminateQueryExecutorTest;
import io.confluent.ksql.rest.server.execution.VariableExecutorTest;
import io.confluent.ksql.rest.server.query.QueryExecutorTest;
import io.confluent.ksql.rest.server.resources.ClusterStatusResourceTest;
import io.confluent.ksql.rest.server.resources.HealthCheckResourceTest;
import io.confluent.ksql.rest.server.resources.HeartbeatResourceTest;
import io.confluent.ksql.rest.server.resources.KsqlResourceTest;
import io.confluent.ksql.rest.server.resources.ServerInfoResourceTest;
import io.confluent.ksql.rest.server.resources.ServerMetadataResourceTest;
import io.confluent.ksql.rest.server.resources.StatusResourceTest;
import io.confluent.ksql.rest.server.resources.WSQueryEndpointTest;
import io.confluent.ksql.rest.server.resources.streaming.PollingSubscriptionTest;
import io.confluent.ksql.rest.server.resources.streaming.PrintSubscriptionTest;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryPublisherTest;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryStreamWriterTest;
import io.confluent.ksql.rest.server.resources.streaming.QueryStreamWriterTest;
import io.confluent.ksql.rest.server.resources.streaming.RecordFormatterTest;
import io.confluent.ksql.rest.server.resources.streaming.SessionUtilTest;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResourceTest;
import io.confluent.ksql.rest.server.resources.streaming.TombstoneFactoryTest;
import io.confluent.ksql.rest.server.resources.streaming.TopicStreamWriterTest;
import io.confluent.ksql.rest.server.restore.KsqlRestoreCommandTopicTest;
import io.confluent.ksql.rest.server.restore.RestoreOptionsTest;
import io.confluent.ksql.rest.server.services.DefaultKsqlClientTest;
import io.confluent.ksql.rest.server.services.InternalKsqlClientFactoryTest;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClientTest;
import io.confluent.ksql.rest.server.validation.PrintTopicValidatorTest;
import io.confluent.ksql.rest.server.validation.PropertyOverriderTest;
import io.confluent.ksql.rest.server.validation.RequestValidatorTest;
import io.confluent.ksql.rest.util.AuthenticationUtilTest;
import io.confluent.ksql.rest.util.ClusterTerminatorTest;
import io.confluent.ksql.rest.util.CommandStoreUtilTest;
import io.confluent.ksql.rest.util.ConcurrencyLimiterTest;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtilTest;
import io.confluent.ksql.rest.util.EntityUtilTest;
import io.confluent.ksql.rest.util.FeatureFlagCheckerTest;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtilsTest;
import io.confluent.ksql.rest.util.KsqlUncaughtExceptionHandlerTest;
import io.confluent.ksql.rest.util.PersistentQueryCleanupImplTest;
import io.confluent.ksql.rest.util.QueryCapacityUtilTest;
import io.confluent.ksql.rest.util.RateLimiterTest;
import io.confluent.ksql.rest.util.RocksDBConfigSetterHandlerTest;
import io.confluent.ksql.rest.util.ScalablePushUtilTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ApiTest.class,
        AuthTest.class,
        BaseApiTest.class,
        CorsTest.class,
        Http2OnlyStreamTest.class,
        ListenersTest.class,
        MaxQueriesTest.class,
        ServerCorsTest.class,
        io.confluent.ksql.api.ServerStateTest.class,
        TlsTest.class,
        ProcessingLogServerUtilsTest.class,
        ImmutabilityTest.class,
        QueryDescriptionFactoryTest.class,
        SourceDescriptionFactoryTest.class,
        HealthCheckAgentTest.class,
        HealthCheckResponseTest.class,
        AuthorizationFunctionalTest.class,
        BackupRollbackIntegrationTest.class,
        ClusterTerminationTest.class,
        CommandTopicFunctionalTest.class,
        HealthCheckResourceFunctionalTest.class,
        HeartbeatAgentFunctionalTest.class,
        KsqlResourceFunctionalTest.class,
        LagReportingAgentFunctionalTest.class,
        PauseResumeIntegrationTest.class,
        PreconditionCheckerIntegrationTest.class,
        ProcessingLogErrorMetricFunctionalTest.class,
        PullQueryFunctionalTest.class,
        PullQueryLimitHARoutingTest.class,
        PullQueryMetricsFunctionalTest.class,
        PullQueryMetricsHttp2FunctionalTest.class,
        PullQueryMetricsWSFunctionalTest.class,
        PullQueryRoutingFunctionalTest.class,
        PullQuerySingleNodeFunctionalTest.class,
        QueryRestartMetricFunctionalTest.class,
        RestApiTest.class,
        ScalablePushQueryFunctionalTest.class,
        ShowQueriesMultiNodeFunctionalTest.class,
        ShowQueriesMultiNodeWithTlsFunctionalTest.class,
        SystemAuthenticationFunctionalTest.class,
        TerminateTransientQueryFunctionalTest.class,
        TransientQueryResourceCleanerIntTest.class,
        CommandRunnerMetricsTest.class,
        CommandRunnerTest.class,
        CommandStoreTest.class,
        CommandTest.class,
        ConfigTopicKeyTest.class,
        DeprecatedStatementsCheckerTest.class,
        DistributingExecutorTest.class,
        InteractiveStatementExecutorTest.class,
        InternalTopicSerdesTest.class,
        KafkaConfigStoreTest.class,
        RecoveryTest.class,
        SequenceNumberFutureStoreTest.class,
        ValidatedCommandFactoryTest.class,
        AssertSchemaExecutorTest.class,
        AssertTopicExecutorTest.class,
        ConnectExecutorTest.class,
        DefaultCommandQueueSyncTest.class,
        DescribeConnectorExecutorTest.class,
        DescribeFunctionExecutorTest.class,
        DropConnectorExecutorTest.class,
        ExplainExecutorTest.class,
        InsertValuesExecutorTest.class,
        ListConnectorPluginsTest.class,
        ListConnectorsExecutorTest.class,
        ListFunctionsExecutorTest.class,
        ListPropertiesExecutorTest.class,
        ListQueriesExecutorTest.class,
        ListSourceExecutorTest.class,
        ListTopicsExecutorTest.class,
        ListTypesExecutorTest.class,
        ListVariablesExecutorTest.class,
        PropertyExecutorTest.class,
        PullQueryMetricsTest.class,
        RemoteHostExecutorTest.class,
        RemoteSourceDescriptionExecutorTest.class,
        RequestHandlerTest.class,
        ScalablePushQueryMetricsTest.class,
        TerminateQueryExecutorTest.class,
        VariableExecutorTest.class,
        QueryExecutorTest.class,
        PollingSubscriptionTest.class,
        PrintSubscriptionTest.class,
        PullQueryPublisherTest.class,
        PullQueryStreamWriterTest.class,
        QueryStreamWriterTest.class,
        RecordFormatterTest.class,
        SessionUtilTest.class,
        StreamedQueryResourceTest.class,
        TombstoneFactoryTest.class,
        TopicStreamWriterTest.class,
        ClusterStatusResourceTest.class,
        HealthCheckResourceTest.class,
        HeartbeatResourceTest.class,
        KsqlResourceTest.class,
        ServerInfoResourceTest.class,
        ServerMetadataResourceTest.class,
        StatusResourceTest.class,
        WSQueryEndpointTest.class,
        KsqlRestoreCommandTopicTest.class,
        RestoreOptionsTest.class,
        DefaultKsqlClientTest.class,
        InternalKsqlClientFactoryTest.class,
        ServerInternalKsqlClientTest.class,
        io.confluent.ksql.rest.server.state.ServerStateTest.class,
        PrintTopicValidatorTest.class,
        PropertyOverriderTest.class,
        RequestValidatorTest.class,
        ActiveHostFilterTest.class,
        BackupReplayFileTest.class,
        CommandTopicBackupImplTest.class,
        CommandTopicTest.class,
        ConnectIntegrationTest.class,
        HeartbeatAgentTest.class,
        InsertionIntegrationTest.class,
        KsqlPlanSchemaTest.class,
        KsqlRestApplicationTest.class,
        KsqlRestConfigTest.class,
        KsqlServerMainTest.class,
        LagReportingAgentTest.class,
        LivenessFilterTest.class,
        LocalCommandsFileTest.class,
        LocalCommandsTest.class,
        MaximumLagFilterTest.class,
        MultiExecutableTest.class,
        PreconditionCheckerTest.class,
        ServerOptionsTest.class,
        ServerUtilTest.class,
        StandaloneExecutorFactoryTest.class,
        StandaloneExecutorFunctionalTest.class,
        StandaloneExecutorTest.class,
        TestKsqlRestAppTest.class,
        AuthenticationUtilTest.class,
        ClusterTerminatorTest.class,
        CommandStoreUtilTest.class,
        ConcurrencyLimiterTest.class,
        DiscoverRemoteHostsUtilTest.class,
        EntityUtilTest.class,
        FeatureFlagCheckerTest.class,
        KsqlInternalTopicUtilsTest.class,
        KsqlUncaughtExceptionHandlerTest.class,
        PersistentQueryCleanupImplTest.class,
        QueryCapacityUtilTest.class,
        RateLimiterTest.class,
        RocksDBConfigSetterHandlerTest.class,
        ScalablePushUtilTest.class,
})
public class MyTestsRestApp {
}
