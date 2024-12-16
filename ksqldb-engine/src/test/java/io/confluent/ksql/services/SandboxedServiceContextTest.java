/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.services.SandboxedSchemaRegistryClient.SandboxSchemaRegistryCache;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public final class SandboxedServiceContextTest {

  private SandboxedServiceContextTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase<SandboxedServiceContext>> getMethodsToTest() {
      return TestMethods.builder(SandboxedServiceContext.class)
          .ignore("getTopicClient")
          .ignore("getKafkaClientSupplier")
          .ignore("getSchemaRegistryClient")
          .ignore("getSchemaRegistryClientFactory")
          .ignore("getConnectClient")
          .ignore("getConsumerGroupClient")
          .ignore("close")
          .build();
    }

    private final TestCase<SandboxedServiceContext> testCase;
    @Mock
    private ServiceContext delegate;
    @Mock
    private KafkaTopicClient delegateTopicClient;
    @Mock
    private SchemaRegistryClient delegateSrClient;
    @Mock
    private KafkaConsumerGroupClient delegateConsumerGroupClient;
    private SandboxedServiceContext sandboxedServiceContext;

    public UnsupportedMethods(final TestCase<SandboxedServiceContext> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      MockitoAnnotations.openMocks(this);

      when(delegate.getTopicClient()).thenReturn(delegateTopicClient);
      when(delegate.getSchemaRegistryClient()).thenReturn(delegateSrClient);
      when(delegate.getConsumerGroupClient()).thenReturn(delegateConsumerGroupClient);

      sandboxedServiceContext = SandboxedServiceContext.create(delegate);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedServiceContext);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private ServiceContext delegate;
    @Mock
    private KafkaTopicClient delegateTopicClient;
    @Mock
    private SchemaRegistryClient delegateSrClient;
    @Mock
    private ConnectClient delegateConnectClient;
    @Mock
    private KafkaConsumerGroupClient delegateConsumerGroupClient;
    private SandboxedServiceContext sandboxedServiceContext;

    @Before
    public void setUp() {
      when(delegate.getTopicClient()).thenReturn(delegateTopicClient);
      when(delegate.getSchemaRegistryClient()).thenReturn(delegateSrClient);
      when(delegate.getConsumerGroupClient()).thenReturn(delegateConsumerGroupClient);
      when(delegate.getConnectClient()).thenReturn(delegateConnectClient);

      sandboxedServiceContext = SandboxedServiceContext.create(delegate);
    }

    @Test
    public void shouldNowWrapTwice() {
      assertThat(SandboxedServiceContext.create(sandboxedServiceContext),
          is(sameInstance(sandboxedServiceContext)));
    }

    @Test
    public void shouldGetSandboxedTopicClient() {
      // When:
      final KafkaTopicClient client = sandboxedServiceContext.getTopicClient();

      // Then:
      assertThat(Proxy.isProxyClass(client.getClass()), is(true));

      // When:
      client.isTopicExists("some topic");

      // Then:
      verify(delegateTopicClient).isTopicExists("some topic");
    }

    @Test
    public void shouldGetSandboxedKafkaClientSupplier() {
      // When:
      final KafkaClientSupplier actual = sandboxedServiceContext.getKafkaClientSupplier();

      // Then:
      assertThat(actual, is(instanceOf(SandboxedKafkaClientSupplier.class)));
    }

    @Test
    public void shouldGetSandboxedSchemaRegistryClient() throws Exception {
      // When:
      final SchemaRegistryClient actual = sandboxedServiceContext.getSchemaRegistryClient();

      // Then:
      assertThat(actual, instanceOf(SandboxSchemaRegistryCache.class));

      // When:
      actual.getLatestSchemaMetadata("some subject");

      // Then:
      verify(delegateSrClient).getLatestSchemaMetadata("some subject");
    }

    @Test
    public void shouldGetSandboxedSchemaRegistryFactory() {
      // When:
      final Supplier<SchemaRegistryClient> factory = sandboxedServiceContext
          .getSchemaRegistryClientFactory();

      // Then:
      assertThat(factory.get(), is(sameInstance(sandboxedServiceContext.getSchemaRegistryClient())));
    }

    @Test
    public void shouldGetSandboxedConnectClient() {
      // When:
      final ConnectClient client = sandboxedServiceContext.getConnectClient();

      // Then:
      assertThat("Expected proxy class", Proxy.isProxyClass(client.getClass()));
    }

    @Test
    public void shouldNotCreateConnectDelegateUnlessCalled() {
      // Then: no delegate connect client called on create()
      verify(delegate, never()).getConnectClient();

      // When:
      sandboxedServiceContext.getConnectClient();

      // Then: now the delegate connect client should have been called
      verify(delegate, times(1)).getConnectClient();
    }

    @Test
    public void shouldNoNothingOnClose() {
      sandboxedServiceContext.close();
    }

  }
}