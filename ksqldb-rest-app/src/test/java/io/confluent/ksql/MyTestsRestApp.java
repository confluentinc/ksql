package io.confluent.ksql;

import io.confluent.ksql.logging.processing.ProcessingLogServerUtilsTest;
import io.confluent.ksql.rest.app.ImmutabilityTest;
import io.confluent.ksql.rest.entity.QueryDescriptionFactoryTest;
import io.confluent.ksql.rest.entity.SourceDescriptionFactoryTest;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgentTest;
import io.confluent.ksql.rest.healthcheck.HealthCheckResponseTest;
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
        ProcessingLogServerUtilsTest.class,
        ImmutabilityTest.class,
        QueryDescriptionFactoryTest.class,
        SourceDescriptionFactoryTest.class,
        HealthCheckAgentTest.class,
        HealthCheckResponseTest.class,
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
