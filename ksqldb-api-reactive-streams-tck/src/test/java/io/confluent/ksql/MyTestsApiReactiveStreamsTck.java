package io.confluent.ksql;

import io.confluent.ksql.api.tck.BaseSubscriberBlackboxVerificationTest;
import io.confluent.ksql.api.tck.BaseSubscriberWhiteboxVerificationTest;
import io.confluent.ksql.api.tck.BlockingQueryPublisherVerificationTest;
import io.confluent.ksql.api.tck.BufferedPublisherVerificationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        BaseSubscriberBlackboxVerificationTest.class,
        BaseSubscriberWhiteboxVerificationTest.class,
        BlockingQueryPublisherVerificationTest.class,
        BufferedPublisherVerificationTest.class
})
public class MyTestsApiReactiveStreamsTck {
}
