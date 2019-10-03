/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.registry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSchemaRegistryClientTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  private SchemaRegistryClient ksqlSchemaRegistryClient;

  @Before
  public void setUp() {
    ksqlSchemaRegistryClient = KsqlSchemaRegistryClient.createProxy(schemaRegistryClient);
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnDeleteSubject()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).deleteSubject("s1");

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.DELETE, "subject: s1"));

    // When
    ksqlSchemaRegistryClient.deleteSubject("s1");
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnGetAllSubjects()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).getAllSubjects();

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.READ, "method: getAllSubjects()"));

    // When
    ksqlSchemaRegistryClient.getAllSubjects();
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnGetById()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).getById(1);

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.READ, "schema_id: 1"));

    // When
    ksqlSchemaRegistryClient.getById(1);
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnGetLatestSchema()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).getLatestSchemaMetadata("s");

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.READ, "subject: s"));

    // When
    ksqlSchemaRegistryClient.getLatestSchemaMetadata("s");
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnRegister()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).register(any(), any());

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.WRITE, "subject: s"));

    // When
    ksqlSchemaRegistryClient.register("s", Schema.create(Schema.Type.STRING));
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnTestCompatibility()
      throws IOException, RestClientException {
    // Given
    doThrow(new RestClientException("", SC_FORBIDDEN, 1))
        .when(schemaRegistryClient).testCompatibility(any(), any());

    // Then
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(messageFor(AclOperation.WRITE, "subject: s"));

    // When
    ksqlSchemaRegistryClient.testCompatibility("s", Schema.create(Schema.Type.STRING));
  }

  private String messageFor(final AclOperation aclOperation, final String str) {
    return String.format("Denied access to %s from Schema Registry %s",
        StringUtils.capitalize(aclOperation.toString().toLowerCase()),
        str);
  }
}
