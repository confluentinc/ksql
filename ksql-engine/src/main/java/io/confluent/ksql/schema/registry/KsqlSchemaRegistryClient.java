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

import static io.confluent.ksql.exception.KsqlSchemaAuthorizationException.Type.METHOD;
import static io.confluent.ksql.exception.KsqlSchemaAuthorizationException.Type.SCHEMA_ID;
import static io.confluent.ksql.exception.KsqlSchemaAuthorizationException.Type.SUBJECT;
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;
import static io.confluent.ksql.util.LimitedProxyBuilder.noParams;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.http.HttpStatus;
import org.apache.kafka.common.acl.AclOperation;

public class KsqlSchemaRegistryClient {
  public static SchemaRegistryClient createProxy(final SchemaRegistryClient schemaRegistryClient) {
    final KsqlSchemaRegistryClient ksqlSchemaRegistryClient =
        new KsqlSchemaRegistryClient(schemaRegistryClient);

    return LimitedProxyBuilder.forClass(SchemaRegistryClient.class)
        .forward("deleteSubject",
            methodParams(String.class),
            ksqlSchemaRegistryClient)
        .forward("getById",
            methodParams(int.class),
            ksqlSchemaRegistryClient)
        .forward("getAllSubjects",
            noParams(),
            ksqlSchemaRegistryClient)
        .forward("getLatestSchemaMetadata",
            methodParams(String.class),
            ksqlSchemaRegistryClient)
        .forward("register",
            methodParams(String.class, Schema.class),
            ksqlSchemaRegistryClient)
        .forward("testCompatibility",
            methodParams(String.class, Schema.class),
            ksqlSchemaRegistryClient)
        .build();
  }

  private final SchemaRegistryClient delegate;

  public KsqlSchemaRegistryClient(final SchemaRegistryClient delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
  }

  public List<Integer> deleteSubject(final String subject) throws IOException, RestClientException {
    try {
      return delegate.deleteSubject(subject);
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(AclOperation.DELETE, SUBJECT, subject);
      }

      throw e;
    }
  }

  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    try {
      return delegate.getAllSubjects();
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(AclOperation.READ, METHOD, "getAllSubjects()");
      }

      throw e;
    }
  }

  public Schema getById(final int id) throws IOException, RestClientException {
    try {
      return delegate.getById(id);
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(
            AclOperation.READ,
            SCHEMA_ID,
            String.valueOf(id)
        );
      }

      throw e;
    }
  }

  public SchemaMetadata getLatestSchemaMetadata(final String subject)
      throws IOException, RestClientException {
    try {
      return delegate.getLatestSchemaMetadata(subject);
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(AclOperation.READ, SUBJECT, subject);
      }

      throw e;
    }
  }

  public int register(final String subject, final Schema schema)
      throws IOException, RestClientException {
    try {
      return delegate.register(subject, schema);
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(AclOperation.WRITE, SUBJECT, subject);
      }

      throw e;
    }
  }

  public boolean testCompatibility(final String subject, final Schema schema)
      throws IOException, RestClientException {
    try {
      return delegate.testCompatibility(subject, schema);
    } catch (final RestClientException e) {
      if (isAuthorizationError(e)) {
        throw new KsqlSchemaAuthorizationException(AclOperation.WRITE, SUBJECT, subject);
      }

      throw e;
    }
  }

  private boolean isAuthorizationError(final RestClientException e) {
    switch (e.getStatus()) {
      case HttpStatus.SC_UNAUTHORIZED:
      case HttpStatus.SC_FORBIDDEN:
        return true;
      default:
        return false;
    }
  }
}
