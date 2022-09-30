package io.confluent.ksql.api;

import io.confluent.ksql.api.auth.AuthenticationPluginHandlerTest;
import io.confluent.ksql.api.auth.BasicCallbackHandlerTest;
import io.confluent.ksql.api.auth.JaasAuthProviderTest;
import io.confluent.ksql.api.auth.KsqlAuthorizationProviderHandlerTest;
import io.confluent.ksql.api.auth.SystemAuthenticationHandlerTest;
import io.confluent.ksql.api.impl.DefaultKsqlSecurityContextProviderTest;
import io.confluent.ksql.api.server.JsonStreamedRowResponseWriterTest;
import io.confluent.ksql.api.server.LoggingHandlerTest;
import io.confluent.ksql.api.server.LoggingRateLimiterTest;
import io.confluent.ksql.api.server.OldApiUtilsTest;
import io.confluent.ksql.api.server.QueryStreamHandlerTest;
import io.confluent.ksql.api.server.ServerUtilsTest;
import io.confluent.ksql.api.server.SlidingWindowRateLimiterTest;
import io.confluent.ksql.api.server.SniHandlerTest;
import io.confluent.ksql.api.util.ApiServerUtilsTest;
import io.confluent.ksql.api.util.ApiSqlValueCoercerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AuthenticationPluginHandlerTest.class,
        BasicCallbackHandlerTest.class,
        JaasAuthProviderTest.class,
        KsqlAuthorizationProviderHandlerTest.class,
        SystemAuthenticationHandlerTest.class,
        DefaultKsqlSecurityContextProviderTest.class,
//        Bazel doesn't support custom security managers, so these need to be re-worked.
//        ApiIntegrationTest.class,
//        CommandTopicMigrationIntegrationTest.class,
//        PullBandwidthThrottleIntegrationTest.class,
//        QuickDegradeAndRestoreCommandTopicIntegrationTest.class,
//        RestoreCommandTopicIntegrationTest.class,
//        RestoreCommandTopicMultipleKafkasIntegrationTest.class,
//        ScalablePushBandwidthThrottleIntegrationTest.class,
        JsonStreamedRowResponseWriterTest.class,
        LoggingHandlerTest.class,
        LoggingRateLimiterTest.class,
        OldApiUtilsTest.class,
        QueryStreamHandlerTest.class,
        ServerUtilsTest.class,
        SlidingWindowRateLimiterTest.class,
        SniHandlerTest.class,
        ApiServerUtilsTest.class,
        ApiSqlValueCoercerTest.class,
        ApiTest.class,
        AuthTest.class,
// Test class can only have one constructor
//        CorsTest.class,
        Http2OnlyStreamTest.class,
        ListenersTest.class,
        MaxQueriesTest.class,
        ServerCorsTest.class,
        io.confluent.ksql.api.ServerStateTest.class,
        TlsTest.class,
})
public class MyTestsRestAppApi {
}
