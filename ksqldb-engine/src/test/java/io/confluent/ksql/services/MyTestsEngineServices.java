package io.confluent.ksql.services;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultConnectClientFactoryTest.class,
        DefaultConnectClientTest.class,
        KafkaTopicClientImplIntegrationTest.class,
        KafkaTopicClientImplTest.class,
        MemoizedSupplierTest.class,
        SandboxConnectClientTest.class,
        SandboxedAdminClientTest.class,
        SandboxedConsumerTest.class,
        SandboxedKafkaClientSupplierTest.class,
        SandboxedKafkaTopicClientTest.class,
        SandboxedProducerTest.class,
        SandboxedSchemaRegistryClientTest.class,
        SandboxedServiceContextTest.class,
        TestServiceContextTest.class,
})
public class MyTestsEngineServices {
}
