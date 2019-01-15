/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class TryKafkaClientSupplierTest {

  private TryKafkaClientSupplierTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(TryKafkaClientSupplier.class)
          .ignore("getAdminClient", Map.class)
          .ignore("getProducer", Map.class)
          .ignore("getConsumer", Map.class)
          .ignore("getRestoreConsumer", Map.class)
          .build();
    }

    private final TestCase<TryKafkaClientSupplier> testCase;
    private TryKafkaClientSupplier tryKafkaClientSupplier;

    public UnsupportedMethods(final TestCase<TryKafkaClientSupplier> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      tryKafkaClientSupplier = new TryKafkaClientSupplier();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(tryKafkaClientSupplier);
    }
  }

  public static class SupportedMethods {

    private final Map<String, Object> config = ImmutableMap.of();
    private TryKafkaClientSupplier tryKafkaClientSupplier;

    @Before
    public void setUp() {
      tryKafkaClientSupplier = new TryKafkaClientSupplier();
    }

    @Test
    public void shouldReturnTryAdminClient() {
      assertThat(tryKafkaClientSupplier.getAdminClient(config),
          is(instanceOf(TryAdminClient.class)));
    }

    @Test
    public void shouldReturnTryProducer() {
      assertThat(tryKafkaClientSupplier.getProducer(config), is(instanceOf(TryProducer.class)));
    }

    @Test
    public void shouldReturnTryConsumer() {
      assertThat(tryKafkaClientSupplier.getConsumer(config), is(instanceOf(TryConsumer.class)));
    }

    @Test
    public void shouldReturnTryRestoreConsumer() {
      assertThat(tryKafkaClientSupplier.getRestoreConsumer(config),
          is(instanceOf(TryConsumer.class)));
    }
  }
}