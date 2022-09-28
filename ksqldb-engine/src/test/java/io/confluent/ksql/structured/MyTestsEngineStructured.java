package io.confluent.ksql.structured;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        QueryContextTest.class,
        SchemaKGroupedStreamTest.class,
        SchemaKGroupedTableTest.class,
        SchemaKSourceFactoryTest.class,
        SchemaKStreamTest.class,
//        SchemaKTableTest.class,
})
public class MyTestsEngineStructured {
}
