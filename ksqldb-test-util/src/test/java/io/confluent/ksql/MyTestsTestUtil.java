package io.confluent.ksql;

import io.confluent.ksql.test.util.ImmutableTesterTest;
import io.confluent.ksql.test.util.ZooKeeperEmbeddedTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ImmutableTesterTest.class,
        ZooKeeperEmbeddedTest.class,
})
public class MyTestsTestUtil {
}