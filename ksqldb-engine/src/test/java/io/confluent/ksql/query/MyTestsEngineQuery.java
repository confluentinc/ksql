package io.confluent.ksql.query;

import io.confluent.ksql.query.id.SequentialQueryIdGeneratorTest;
import io.confluent.ksql.query.id.SpecificQueryIdGeneratorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        SequentialQueryIdGeneratorTest.class,
        SpecificQueryIdGeneratorTest.class,
        AuthorizationClassifierTest.class,
        KafkaStreamsQueryValidatorTest.class,
        KsqlFunctionClassifierTest.class,
        KsqlSerializationClassifierTest.class,
        MissingSubjectClassifierTest.class,
        MissingTopicClassifierTest.class,
        PullQueryWriteStreamTest.class,
        QueryBuilderTest.class,
        QueryRegistryImplTest.class,
        RegexClassifierTest.class,
        SchemaAuthorizationClassifierTest.class,
        TransientQueryQueueTest.class,
})
public class MyTestsEngineQuery {
}
