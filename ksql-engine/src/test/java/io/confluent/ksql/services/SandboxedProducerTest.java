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
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class SandboxedProducerTest {

  private SandboxedProducerTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(Producer.class)
          .ignore("close")
          .ignore("close", long.class, TimeUnit.class)
          .ignore("close", Duration.class)
          .build();
    }

    private final TestCase<Producer<Long, String>> testCase;
    private Producer<Long, String> sandboxedProducer;

    public UnsupportedMethods(final TestCase<Producer<Long, String>> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedProducer = SandboxedProducer.createProxy();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedProducer);
    }
  }

  public static class SupportedMethods {

    private Producer<Long, String> sandboxedProducer;

    @Before
    public void setUp() {
      sandboxedProducer = SandboxedProducer.createProxy();
    }

    @Test
    public void shouldDoNothingOnCloseWithNoArgs() {
      sandboxedProducer.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDoNothingOnClose() {
      sandboxedProducer.close();
      sandboxedProducer.close(1, TimeUnit.MILLISECONDS);
      sandboxedProducer.close(Duration.ofMillis(1));
    }
  }
}