package io.confluent.ksql;

//import io.confluent.ksql.api.client.impl.ClientImplTest;
import io.confluent.ksql.api.client.impl.ClientOptionsImplTest;
import io.confluent.ksql.api.client.impl.ColumnTypeImplTest;
import io.confluent.ksql.api.client.impl.ConnectorDescriptionImplTest;
import io.confluent.ksql.api.client.impl.ConnectorInfoImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//        ClientImplTest.class,
        ClientOptionsImplTest.class,
        ColumnTypeImplTest.class,
        ConnectorDescriptionImplTest.class,
        ConnectorInfoImplTest.class
        // todo: add more tests
})
public class MyTestsApiClient {
}
