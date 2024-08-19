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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.util.Collection;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.hc.core5.http.HttpStatus;
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
    public static Collection<TestCase<SchemaRegistryClient>> getMethodsToTest() {
      return TestMethods.builder(SchemaRegistryClient.class)
          // Only add methods in here which are NOT handled by the proxy
          // when adding, ensure you also add a suitable test to SupportedMethods below.
          .ignore("register", String.class, Schema.class)
          .ignore("register", String.class, ParsedSchema.class)
          .ignore("register", String.class, ParsedSchema.class, boolean.class)
          .ignore("register", String.class, Schema.class, int.class, int.class)
          .ignore("register", String.class, ParsedSchema.class, int.class, int.class)
          .ignore("getLatestSchemaMetadata", String.class)
          .ignore("registerWithResponse", String.class, ParsedSchema.class, boolean.class)
          .ignore("registerWithResponse", String.class, ParsedSchema.class, boolean.class, boolean.class)
          .ignore("getSchemaBySubjectAndId", String.class, int.class)
          .ignore("testCompatibility", String.class, Schema.class)
          .ignore("testCompatibility", String.class, ParsedSchema.class)
          .ignore("deleteSubject", String.class)
          .ignore("getAllSubjects")
          .ignore("getId", String.class, ParsedSchema.class)
          .ignore("getId", String.class, ParsedSchema.class, boolean.class)
          .ignore("getId", String.class, Schema.class)
          .ignore("getVersion", String.class, ParsedSchema.class)
          .ignore("getSchemaById", int.class)
          .build();
    }

    private final TestCase<SchemaRegistryClient> testCase;
    private SchemaRegistryClient sandboxedSchemaRegistryClient;

    public UnsupportedMethods(final TestCase<SchemaRegistryClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedSchemaRegistryClient = SandboxedSchemaRegistryClient.createProxy(
          mock(SchemaRegistryClient.class));
    }

    @Test
    public void shouldThrowOnUnsupportedOperation() {
      assertThrows(
          UnsupportedOperationException.class,
          () -> testCase.invokeMethod(sandboxedSchemaRegistryClient)
      );
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private SchemaRegistryClient delegate;
    @Mock
    private AvroSchema schema;
    @Mock
    private ParsedSchema parsedSchema;
    @Mock
    private SchemaMetadata schemaMetadata;
    private SchemaRegistryClient sandboxedClient;

    @Before
    public void setUp() {
      sandboxedClient = SandboxedSchemaRegistryClient.createProxy(delegate);
    }

    @Test
    public void shouldGetLatestSchemaMetadata() throws Exception {
      // Given:
      when(delegate.getLatestSchemaMetadata("some subject")).thenReturn(schemaMetadata);

      // When:
      final SchemaMetadata actual = sandboxedClient
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
      final boolean first = sandboxedClient.testCompatibility("some subject", schema);
      final boolean second = sandboxedClient.testCompatibility("some subject", schema);

      // Then:
      assertThat(first, is(true));
      assertThat(second, is(false));
    }

    @Test
    public void shouldGetAllSubjects() throws Exception {
      // Given:
      when(delegate.getAllSubjects()).thenReturn(ImmutableSet.of("foo"));

      // When:
      final Collection<String> subjects = sandboxedClient.getAllSubjects();

      // Then:
      assertThat(subjects, is(ImmutableSet.of("foo")));
    }

    @Test
    public void shouldSwallowDeleteSubject() throws Exception {
      // When:
      sandboxedClient.deleteSubject("some subject");

      // Then:
      verifyNoMoreInteractions(delegate);
    }

    @Test
    public void shouldSwallowRegister() throws Exception {
      // When:
      sandboxedClient.register("some subject", schema);
      sandboxedClient.register("some subject", schema, 1, 1);

      // Then:
      verifyNoMoreInteractions(delegate);
    }

    @Test
    public void shouldGetVersion() throws Exception {
      // Given:
      when(delegate.getVersion("some subject", schema)).thenReturn(6);

      // When:
      final int version = sandboxedClient.getVersion("some subject", schema);

      // Then:
      assertThat(version, is(6));
    }

    @Test
    public void shouldRegisterWithResponse() throws Exception {
      // When:
      final RegisterSchemaResponse response = sandboxedClient
          .registerWithResponse("some subject", schema, false);

      // Then:
      assertThat(response, is(notNullValue()));
      assertThat(response.getId(), is(1));
    }

    @Test
    public void shouldGetSchemaById() throws Exception {
      // Given:
      when(delegate.getSchemaById(anyInt())).thenReturn(parsedSchema);

      // When:
      final ParsedSchema retSchema = sandboxedClient.getSchemaById(1);

      // Then:
      assertThat(retSchema, is(parsedSchema));
    }

    @Test
    public void shouldGetId() throws Exception {
      // Given:
      when(delegate.getId(anyString(), any(ParsedSchema.class)))
          .thenReturn(123)
          .thenReturn(124)
          .thenReturn(125); // we swallow the third boolean argument in the implementation

      // When:
      final int id = sandboxedClient.getId("some subject", schema);
      final int id1 = sandboxedClient.getId("some subject", parsedSchema);
      final int id2 = sandboxedClient.getId("some subject", parsedSchema, true);

      // Then:
      assertThat(id, is(123));
      assertThat(id1, is(124));
      assertThat(id2, is(125));
    }

    @Test
    public void shouldGetIdFromCache() throws Exception {
      // Given:
      final RestClientException exception = mock(RestClientException.class);
      when(exception.getStatus()).thenReturn(HttpStatus.SC_NOT_FOUND);
      when(delegate.getId(anyString(), any(ParsedSchema.class))).thenThrow(exception);

      final int newId = sandboxedClient.register("newSubject", parsedSchema);

      // When:
      final int id = sandboxedClient.getId("newSubject", parsedSchema);

      // Then:
      assertThat(id, is(newId));

    }
  }
}