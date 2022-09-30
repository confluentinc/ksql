package io.confluent.ksql.rest.server.computation;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
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
})
public class MyTestsRestAppServerComputation {
}
