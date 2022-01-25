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

import static io.confluent.ksql.util.LimitedProxyBuilder.anyParams;
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;

/**
 * SchemaRegistryClient used when trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedSchemaRegistryClient {

  static SchemaRegistryClient createProxy(final SchemaRegistryClient delegate) {
    Objects.requireNonNull(delegate, "delegate");
    final SandboxSchemaRegistryCache sandboxSchemaRegistryCache
        = new SandboxSchemaRegistryCache(delegate);

    return LimitedProxyBuilder.forClass(SchemaRegistryClient.class)
        .forward(
            "register",
            methodParams(String.class, ParsedSchema.class),
            sandboxSchemaRegistryCache)
        .swallow(
            "getId",
            anyParams(),
            123)
        .forward(
            "getAllSubjects",
            methodParams(),
            sandboxSchemaRegistryCache)
        .forward(
            "getSchemaById",
            methodParams(int.class),
            sandboxSchemaRegistryCache)
        .forward(
            "getLatestSchemaMetadata",
            methodParams(String.class),
            sandboxSchemaRegistryCache)
        .forward(
            "getSchemaBySubjectAndId",
            methodParams(String.class, int.class),
            sandboxSchemaRegistryCache)
        .forward(
            "testCompatibility",
            methodParams(String.class, ParsedSchema.class),
            sandboxSchemaRegistryCache)
        .swallow(
            "deleteSubject",
            methodParams(String.class),
            Collections.emptyList())
        .forward(
            "getVersion",
            methodParams(String.class, ParsedSchema.class),
            sandboxSchemaRegistryCache)
        .build();
  }

  private SandboxedSchemaRegistryClient() {
  }

  private static final class SandboxSchemaRegistryCache implements SchemaRegistryClient {
    private final SchemaRegistryClient delegate;

    private int nextSchemaId = Integer.MAX_VALUE;
    private final Map<String, ParsedSchema> subjectCache = new HashMap<>();
    private final Map<String, Integer> subjectToId = new HashMap<>();
    private final Map<Integer, ParsedSchema> idCache = new HashMap<>();

    private SandboxSchemaRegistryCache(final SchemaRegistryClient delegate) {
      this.delegate = delegate;
    }

    @Override
    public Optional<ParsedSchema> parseSchema(
        final String schemaType,
        final String schemaString,
        final List<SchemaReference> references) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int register(final String subject, final ParsedSchema parsedSchema) {
      if (subjectCache.containsKey(subject)) {
        throw new IllegalStateException("Subject '" + subject + "' already in use.");
      }
      final int schemaId = nextSchemaId--;
      subjectCache.put(subject, parsedSchema);
      subjectToId.put(subject, schemaId);
      idCache.put(schemaId, parsedSchema);
      return schemaId;
    }

    @Override
    public int register(
        final String subject,
        final ParsedSchema parsedSchema,
        final int version,
        final int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ParsedSchema getSchemaById(final int id) throws RestClientException, IOException {
      try {
        return delegate.getSchemaById(id);
      } catch (RestClientException e) {
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          final ParsedSchema schema = idCache.get(id);
          if (schema != null) {
            return schema;
          }

        }
        throw e;
      }
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(final String subject, final int id)
        throws RestClientException, IOException {

      try {
        return delegate.getSchemaBySubjectAndId(subject, id);
      } catch (final RestClientException e) {
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          final ParsedSchema schemaByName = subjectCache.get(subject);
          final ParsedSchema schemaById = idCache.get(id);
          if (schemaByName != null && schemaByName == schemaById) {
            return schemaByName;
          }
        }
        throw e;
      }
    }

    @Override
    public Collection<String> getAllSubjectsById(final int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(final String subject)
        throws RestClientException, IOException {

      try {
        return delegate.getLatestSchemaMetadata(subject);
      } catch (final RestClientException e) {
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND && subjectCache.containsKey(subject)) {
          return new SchemaMetadata(subjectToId.get(subject), 1, "dummy");
        }
        throw e;
      }
    }

    @Override
    public SchemaMetadata getSchemaMetadata(final String subject, final int version) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getVersion(final String subject, final ParsedSchema parsedSchema) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> getAllVersions(final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean testCompatibility(final String subject, final ParsedSchema parsedSchema) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String updateCompatibility(final String subject, final String compatibility) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getCompatibility(final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String setMode(final String mode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String setMode(final String mode, final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getMode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getMode(final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> getAllSubjects() throws RestClientException, IOException {
      final Collection<String> allSubjects = delegate.getAllSubjects();
      allSubjects.addAll(subjectCache.keySet());
      return allSubjects;
    }

    @Override
    public int getId(final String subject, final ParsedSchema parsedSchema) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }
  }
}
