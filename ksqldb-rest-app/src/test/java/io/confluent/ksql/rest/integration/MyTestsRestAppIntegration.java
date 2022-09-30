package io.confluent.ksql.rest.integration;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//        AuthorizationFunctionalTest.class,
//        BackupRollbackIntegrationTest.class,
//        ClusterTerminationTest.class,
//        CommandTopicFunctionalTest.class,
//        HealthCheckResourceFunctionalTest.class,
//        HeartbeatAgentFunctionalTest.class,
//        KsqlResourceFunctionalTest.class,
//        LagReportingAgentFunctionalTest.class,
//        PauseResumeIntegrationTest.class,
        PreconditionCheckerIntegrationTest.class,
        ProcessingLogErrorMetricFunctionalTest.class,
//        PullQueryFunctionalTest.class,
//        PullQueryLimitHARoutingTest.class,
        PullQueryMetricsFunctionalTest.class,
        PullQueryMetricsHttp2FunctionalTest.class,
        PullQueryMetricsWSFunctionalTest.class,
        PullQueryRoutingFunctionalTest.class,
        PullQuerySingleNodeFunctionalTest.class,
        QueryRestartMetricFunctionalTest.class,
//        RestApiTest.class,
//        ScalablePushQueryFunctionalTest.class,
//        ShowQueriesMultiNodeFunctionalTest.class,
//        ShowQueriesMultiNodeWithTlsFunctionalTest.class,
//        SystemAuthenticationFunctionalTest.class,
//        TerminateTransientQueryFunctionalTest.class,
//        TransientQueryResourceCleanerIntTest.class,
})
public class MyTestsRestAppIntegration {
}
