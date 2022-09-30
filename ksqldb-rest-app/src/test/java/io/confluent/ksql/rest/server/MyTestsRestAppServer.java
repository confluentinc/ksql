package io.confluent.ksql.rest.server;

import io.confluent.ksql.rest.server.query.QueryExecutorTest;
import io.confluent.ksql.rest.server.restore.KsqlRestoreCommandTopicTest;
import io.confluent.ksql.rest.server.restore.RestoreOptionsTest;
import io.confluent.ksql.rest.server.validation.PrintTopicValidatorTest;
import io.confluent.ksql.rest.server.validation.PropertyOverriderTest;
import io.confluent.ksql.rest.server.validation.RequestValidatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        QueryExecutorTest.class,
        KsqlRestoreCommandTopicTest.class,
        RestoreOptionsTest.class,
        io.confluent.ksql.rest.server.state.ServerStateTest.class,
        PrintTopicValidatorTest.class,
        PropertyOverriderTest.class,
        RequestValidatorTest.class,
        ActiveHostFilterTest.class,
        BackupReplayFileTest.class,
        CommandTopicBackupImplTest.class,
        CommandTopicTest.class,
//        ConnectIntegrationTest.class,
        HeartbeatAgentTest.class,
//        InsertionIntegrationTest.class,
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
//        StandaloneExecutorFactoryTest.class,
//        StandaloneExecutorFunctionalTest.class,
        StandaloneExecutorTest.class,
//        TestKsqlRestAppTest.class,
})
public class MyTestsRestAppServer {
}
