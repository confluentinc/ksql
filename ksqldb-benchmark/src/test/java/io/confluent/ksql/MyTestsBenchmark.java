package io.confluent.ksql;

import io.confluent.ksql.benchmark.SerdeBenchmarkTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        SerdeBenchmarkTest.class
})
public class MyTestsBenchmark {
}
