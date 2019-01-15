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
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class TryProducerTest {

  private TryProducerTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(TryProducer.class)
          .ignore("close")
          .ignore("close", long.class, TimeUnit.class)
          .build();
    }

    private final TestCase<TryProducer<Long, String>> testCase;
    private TryProducer<Long, String> tryProducer;

    public UnsupportedMethods(final TestCase<TryProducer<Long, String>> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      tryProducer = new TryProducer<>();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(tryProducer);
    }
  }

  public static class SupportedMethods {

    private TryProducer<Long, String> tryProducer;

    @Before
    public void setUp() {
      tryProducer = new TryProducer<>();
    }

    @Test
    public void shouldDoNothingOnCloseWithNoArgs() {
      tryProducer.close();
    }

    @Test
    public void shouldDoNothingOnClose() {
      tryProducer.close(1, TimeUnit.MILLISECONDS);
    }
  }
}