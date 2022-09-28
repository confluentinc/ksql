package io.confluent.ksql.util;

import io.confluent.ksql.KsqlContextTestUtilTest;
import io.confluent.ksql.util.json.JsonPathTokenizerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        JsonPathTokenizerTest.class,
        ArrayUtilTest.class,
        BinPackedPersistentQueryMetadataImplTest.class,
        ExecutorUtilTest.class,
        FileWatcherTest.class,
        KafkaStreamsThreadErrorTest.class,
        LimitedProxyBuilderTest.class,
        PersistentQueryMetadataTest.class,
        PlanSummaryTest.class,
        QueryLoggerUtilTest.class,
        QueryMetadataTest.class,
        SandboxedPersistentQueryMetadataImplTest.class,
        SandboxedSharedKafkaStreamsRuntimeImplTest.class,
        SandboxedTransientQueryMetadataTest.class,
        ScalablePushQueryMetadataTest.class,
        SharedKafkaStreamsRuntimeImplTest.class,
        TransientQueryMetadataTest.class,
        KsqlContextTestUtilTest.class,
})
public class MyTestsEngineUtil {
}
