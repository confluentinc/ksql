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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

  static final class SandboxSchemaRegistryCache implements SchemaRegistryClient {
    // we use `MockSchemaRegistryClient` as a cache inside the sandbox to store
    // newly registered schemas (without polluting the actual SR)
    // this allows dependent statements to execute successfully inside the sandbox
    private final MockSchemaRegistryClient sandboxCacheClient = new MockSchemaRegistryClient();
    // client to talk to the actual SR
    private final SchemaRegistryClient srClient;

    private SandboxSchemaRegistryCache(final SchemaRegistryClient delegate) {
      this.srClient = delegate;
    }

    @Override
    public Ticker ticker() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String tenant() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ParsedSchema> parseSchema(
        final String schemaType,
        final String schemaString,
        final List<SchemaReference> references) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RegisterSchemaResponse registerWithResponse(
        final String subject, final ParsedSchema schema, final boolean normalize)
        throws IOException, RestClientException {
      return sandboxCacheClient.registerWithResponse(subject, schema, normalize);
    }

    @Override
    public int register(final String subject, final ParsedSchema parsedSchema)
        throws RestClientException, IOException {
      return sandboxCacheClient.register(subject, parsedSchema);
    }

    @Override
    public int register(final String subject, final ParsedSchema schema, final boolean normalize)
        throws RestClientException, IOException {
      return sandboxCacheClient.register(subject, schema, normalize);
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
        return srClient.getSchemaById(id);
      } catch (RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          return sandboxCacheClient.getSchemaById(id);
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
        return srClient.getSchemaBySubjectAndId(subject, id);
      } catch (final RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          return sandboxCacheClient.getSchemaBySubjectAndId(subject, id);
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
        return srClient.getLatestSchemaMetadata(subject);
      } catch (final RestClientException e) {
        // if we don't find the schema metadata in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          return sandboxCacheClient.getLatestSchemaMetadata(subject);
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
      try {
        return srClient.getVersion(subject, parsedSchema);
      } catch (final RestClientException e) {
        // if we don't find the version in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          return sandboxCacheClient.getVersion(subject, parsedSchema);
        }
        throw e;
      }
    }

    @Override
    public List<Integer> getAllVersions(final String subject) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean testCompatibility(final String subject, final ParsedSchema parsedSchema)
        throws RestClientException, IOException {
      return srClient.testCompatibility(subject, parsedSchema)
          && sandboxCacheClient.testCompatibility(subject, parsedSchema);
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
      final Collection<String> allSubjects = new HashSet<>(srClient.getAllSubjects());
      allSubjects.addAll(sandboxCacheClient.getAllSubjects());
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
        return srClient.getId(subject, parsedSchema);
      } catch (final RestClientException e) {
        // if we don't find the schema in SR, we try to get it from the sandbox cache
        if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
          return sandboxCacheClient.getId(subject, parsedSchema);
        }
        throw e;
      }
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }
  }
}
