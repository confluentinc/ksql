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
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.util.Collection;
import java.util.Objects;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public final class SandboxedSchemaRegistryClientTest {

  private SandboxedSchemaRegistryClientTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(SandboxedSchemaRegistryClient.class)
          .ignore("getLatestSchemaMetadata", String.class)
          .ignore("testCompatibility", String.class, Schema.class)
          .build();
    }

    private final TestCase<SandboxedSchemaRegistryClient> testCase;
    private SandboxedSchemaRegistryClient sandboxedSchemaRegistryClient;

    public UnsupportedMethods(final TestCase<SandboxedSchemaRegistryClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedSchemaRegistryClient = new SandboxedSchemaRegistryClient(mock(SchemaRegistryClient.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedSchemaRegistryClient);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private SchemaRegistryClient delegate;
    @Mock
    private Schema schema;
    @Mock
    private SchemaMetadata schemaMetadata;
    private SandboxedSchemaRegistryClient sandboxedSchemaRegistryClient;

    @Before
    public void setUp() {
      sandboxedSchemaRegistryClient = new SandboxedSchemaRegistryClient(delegate);
    }

    @Test
    public void shouldGetLatestSchemaMetadata() throws Exception {
      // Given:
      when(delegate.getLatestSchemaMetadata("some subject")).thenReturn(schemaMetadata);

      // When:
      final SchemaMetadata actual = sandboxedSchemaRegistryClient
          .getLatestSchemaMetadata("some subject");

      // Then:
      assertThat(actual, is(schemaMetadata));
    }

    @Test
    public void shouldTestCompatibility() throws Exception {
      // Given:
      when(delegate.testCompatibility("some subject", schema))
          .thenReturn(true)
          .thenReturn(false);

      // When:
      final boolean first = sandboxedSchemaRegistryClient.testCompatibility("some subject", schema);
      final boolean second = sandboxedSchemaRegistryClient.testCompatibility("some subject", schema);

      // Then:
      assertThat(first, is(true));
      assertThat(second, is(false));
    }
  }
}