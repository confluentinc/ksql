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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;

/**
 * SchemaRegistryClient used when trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
@SuppressWarnings("deprecation")
class TrySchemaRegistryClient implements SchemaRegistryClient {

  private final SchemaRegistryClient delegate;

  TrySchemaRegistryClient(final SchemaRegistryClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public int register(final String subject, final Schema schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getByID(final int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getById(final int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getBySubjectAndID(final String subject, final int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getBySubjectAndId(final String subject, final int id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(final String subject)
      throws IOException, RestClientException {
    return delegate.getLatestSchemaMetadata(subject);
  }

  @Override
  public SchemaMetadata getSchemaMetadata(final String subject, final int version) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getVersion(final String subject, final Schema schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> getAllVersions(final String subject) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean testCompatibility(
      final String subject,
      final Schema schema
  ) throws IOException, RestClientException {
    return delegate.testCompatibility(subject, schema);
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
  public Collection<String> getAllSubjects() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getId(final String subject, final Schema schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> deleteSubject(final String subject) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> deleteSubject(
      final Map<String, String> requestProperties,
      final String subject
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer deleteSchemaVersion(final String subject, final String version) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer deleteSchemaVersion(
      final Map<String, String> requestProperties,
      final String subject,
      final String version
  ) {
    throw new UnsupportedOperationException();
  }
}
