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

import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public final class SandboxedAdminClientTest {

  private SandboxedAdminClientTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase<Admin>> getMethodsToTest() {
      return TestMethods.builder(Admin.class)
          .ignore("close")
          .ignore("close", Duration.class)
          .ignore("close", long.class, TimeUnit.class)
          .setDefault(ElectLeadersOptions.class, new ElectLeadersOptions())
          .build();
    }

    private final TestCase<AdminClient> testCase;
    private AdminClient sandboxedAdminClient;

    public UnsupportedMethods(final TestCase<AdminClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedAdminClient = new SandboxedAdminClient();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedAdminClient);
    }
  }

  public static class SupportedMethods {

    private AdminClient sandboxedAdminClient;

    @Before
    public void setUp() {
      sandboxedAdminClient = new SandboxedAdminClient();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDoNothingOnClose() {
      sandboxedAdminClient.close();
      sandboxedAdminClient.close(1, TimeUnit.MILLISECONDS);
      sandboxedAdminClient.close(Duration.ofMillis(1));
    }
  }
}