package io.confluent.ksql.rest.server.services;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultKsqlClientTest.class,
        InternalKsqlClientFactoryTest.class,
        ServerInternalKsqlClientTest.class
})
public class MyTestsRestAppServerServices {
}
