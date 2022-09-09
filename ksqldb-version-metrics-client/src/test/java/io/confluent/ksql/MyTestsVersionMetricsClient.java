package io.confluent.ksql;

import io.confluent.ksql.version.metrics.ImmutabilityTest;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgentTest;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerResponseHandlerTest;
import io.confluent.ksql.version.metrics.VersionCheckerIntegrationTest;
import io.confluent.ksql.version.metrics.collector.BasicCollectorTest;
import io.confluent.ksql.version.metrics.collector.KsqlModuleTypeTest;
import io.confluent.support.metrics.BaseSupportConfigTest;
import io.confluent.support.metrics.PhoneHomeConfigTest;
import io.confluent.support.metrics.common.FilterTest;
import io.confluent.support.metrics.common.UuidTest;
import io.confluent.support.metrics.common.time.TimeUtilsTest;
import io.confluent.support.metrics.submitters.ConfluentSubmitterTest;
import io.confluent.support.metrics.utils.JitterTest;
import io.confluent.support.metrics.utils.StringUtilsTest;
import io.confluent.support.metrics.utils.WebClientTest;
import io.confluent.support.metrics.validate.MetricsValidationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        BasicCollectorTest.class,
        KsqlModuleTypeTest.class,
        ImmutabilityTest.class,
        KsqlVersionCheckerAgentTest.class,
        KsqlVersionCheckerResponseHandlerTest.class,
        VersionCheckerIntegrationTest.class,
        TimeUtilsTest.class,
        FilterTest.class,
        UuidTest.class,
        ConfluentSubmitterTest.class,
        JitterTest.class,
        StringUtilsTest.class,
        WebClientTest.class,
        MetricsValidationTest.class,
        BaseSupportConfigTest.class,
        PhoneHomeConfigTest.class,
})
public class MyTestsVersionMetricsClient {
}
