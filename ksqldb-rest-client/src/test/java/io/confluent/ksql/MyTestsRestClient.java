package io.confluent.ksql;

import io.confluent.ksql.rest.client.ImmutabilityTest;
import io.confluent.ksql.rest.client.KsqlClientTest;
import io.confluent.ksql.rest.client.KsqlClientUtilTest;
import io.confluent.ksql.rest.client.KsqlRestClientTest;
import io.confluent.ksql.rest.client.KsqlTargetTest;
import io.confluent.ksql.rest.client.KsqlTargetUtilTest;
import io.confluent.ksql.rest.entity.StreamedRowTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ImmutabilityTest.class,
        KsqlClientTest.class,
        KsqlClientUtilTest.class,
        KsqlRestClientTest.class,
        KsqlTargetTest.class,
        KsqlTargetUtilTest.class,
        StreamedRowTest.class
})
public class MyTestsRestClient {
}
