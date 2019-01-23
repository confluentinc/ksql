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

import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class SandboxedConsumerTest {

  private SandboxedConsumerTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(Consumer.class)
          .ignore("unsubscribe")
          .ignore("close")
          .ignore("close", long.class, TimeUnit.class)
          .ignore("close", Duration.class)
          .ignore("wakeup")
          .setDefault(TopicPartition.class, new TopicPartition("t", 1))
          .build();
    }

    private final TestCase<Consumer<Long, String>> testCase;
    private Consumer<Long, String> sandboxedConsumer;

    public UnsupportedMethods(final TestCase<Consumer<Long, String>> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedConsumer = SandboxedConsumer.createProxy();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedConsumer);
    }
  }

  public static class SupportedMethods {

    private Consumer<Long, String> sandboxedConsumer;

    @Before
    public void setUp() {
      sandboxedConsumer = SandboxedConsumer.createProxy();
    }

    @Test
    public void shouldDoNothingOnUnsubscribe() {
      sandboxedConsumer.unsubscribe();
    }

    @Test
    public void shouldDoNothingOnCloseWithNoArgs() {
      sandboxedConsumer.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDoNothingOnCloseWithTimeUnit() {
      sandboxedConsumer.close(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldDoNothingOnCloseWithDuration() {
      sandboxedConsumer.close(Duration.ofMillis(1));
    }

    @Test
    public void shouldDoNothingOnWakeUp() {
      sandboxedConsumer.wakeup();
    }
  }
}