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

import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
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
    return new SandboxSchemaRegistryCache(delegate);
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
      return -1; // swallow
    }

    @Deprecated
    @Override
    public Schema getById(final int id) {
      throw new UnsupportedOperationException();

    }

    @Override
    public ParsedSchema getSchemaById(final int id) throws RestClientException, IOException {
      try {
        return delegate.getSchemaById(id);
      } catch (RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          final ParsedSchema schema = idCache.get(id);
          if (schema != null) {
            return schema;
          }

        }
        throw e;
      }
    }

    @Deprecated
    @Override
    public Schema getBySubjectAndId(final String subject, final int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(final String subject, final int id)
        throws RestClientException, IOException {

      try {
        return delegate.getSchemaBySubjectAndId(subject, id);
      } catch (final RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
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
        // if we don't find the schema metadata in SR, but the subject is registered inside
        // the sandbox, we return mocked metadata.
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

    @Deprecated
    @Override
    public int getVersion(final String subject, final Schema parsedSchema)
        throws RestClientException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getVersion(final String subject, final ParsedSchema parsedSchema)
        throws RestClientException, IOException {
      return delegate.getVersion(subject, parsedSchema);
    }

    @Override
    public List<Integer> getAllVersions(final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean testCompatibility(final String subject, final ParsedSchema parsedSchema)
        throws RestClientException, IOException {
      return delegate.testCompatibility(subject, parsedSchema);
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
    public List<Integer> deleteSubject(final String subject)
        throws IOException, RestClientException {
      return null; // swallow
    }

    @Override
    public List<Integer> deleteSubject(final String subject, final boolean isPermanent) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> getAllSubjects() throws RestClientException, IOException {
      final Collection<String> allSubjects = new HashSet<>(delegate.getAllSubjects());
      allSubjects.addAll(subjectCache.keySet());
      return ImmutableSet.copyOf(allSubjects);
    }

    @Override
    public int getId(final String subject, final ParsedSchema schema, final boolean normalize)
        throws IOException, RestClientException {
      return getId(subject, schema);
    }

    @Override
    public int getId(final String subject, final ParsedSchema parsedSchema)
        throws RestClientException, IOException {
      try {
        return delegate.getId(subject, parsedSchema);
      } catch (final RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND && subjectToId.containsKey(subject)) {
          return subjectToId.get(subject);
        }
        throw e;
      }
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }
  }
}
