package io.confluent.ksql;


import io.confluent.ksql.datagen.DataGenFunctionalTest;
import io.confluent.ksql.datagen.DataGenProducerTest;
import io.confluent.ksql.datagen.DataGenTest;
import io.confluent.ksql.datagen.GeneratorTest;
import io.confluent.ksql.datagen.RowGeneratorTest;
import io.confluent.ksql.datagen.SessionManagerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DataGenFunctionalTest.class,
//        DataGenProducerTest.class,
        DataGenTest.class,
//        todo issue finding resources to run these tests even though the resources are there
//        GeneratorTest.class,
//        RowGeneratorTest.class,
        SessionManagerTest.class,
})
public class MyTestsExamples {
}
