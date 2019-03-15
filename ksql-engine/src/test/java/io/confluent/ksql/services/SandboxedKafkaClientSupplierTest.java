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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class SandboxedKafkaClientSupplierTest {

  private SandboxedKafkaClientSupplierTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase<SandboxedKafkaClientSupplier>> getMethodsToTest() {
      return TestMethods.builder(SandboxedKafkaClientSupplier.class)
          .ignore("getAdminClient", Map.class)
          .ignore("getProducer", Map.class)
          .ignore("getConsumer", Map.class)
          .ignore("getRestoreConsumer", Map.class)
          .build();
    }

    private final TestCase<SandboxedKafkaClientSupplier> testCase;
    private SandboxedKafkaClientSupplier sandboxedKafkaClientSupplier;

    public UnsupportedMethods(final TestCase<SandboxedKafkaClientSupplier> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedKafkaClientSupplier = new SandboxedKafkaClientSupplier();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedKafkaClientSupplier);
    }
  }

  public static class SupportedMethods {

    private final Map<String, Object> config = ImmutableMap.of();
    private SandboxedKafkaClientSupplier sandboxedKafkaClientSupplier;

    @Before
    public void setUp() {
      sandboxedKafkaClientSupplier = new SandboxedKafkaClientSupplier();
    }

    @Test
    public void shouldReturnTryAdminClient() {
      assertThat(sandboxedKafkaClientSupplier.getAdminClient(config),
          is(instanceOf(SandboxedAdminClient.class)));
    }

    @Test
    public void shouldReturnSandboxProxyProducer() {
      final Producer<byte[], byte[]> producer = sandboxedKafkaClientSupplier.getProducer(config);

      assertThat(Proxy.isProxyClass(producer.getClass()), is(true));
    }

    @Test
    public void shouldReturnSandboxProxyConsumer() {
      final Consumer<byte[], byte[]> consumer = sandboxedKafkaClientSupplier.getConsumer(config);

      assertThat(Proxy.isProxyClass(consumer.getClass()), is(true));
    }

    @Test
    public void shouldReturnSandboxProxyRestoreConsumer() {
      final Consumer<byte[], byte[]> consumer = sandboxedKafkaClientSupplier
          .getRestoreConsumer(config);

      assertThat(Proxy.isProxyClass(consumer.getClass()), is(true));
    }
  }
}