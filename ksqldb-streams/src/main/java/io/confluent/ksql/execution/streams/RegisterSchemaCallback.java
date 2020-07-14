/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.util.KsqlConstants;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RegisterSchemaCallback implements StaticTopicSerde.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterSchemaCallback.class);
  private final SchemaRegistryClient srClient;

  RegisterSchemaCallback(final SchemaRegistryClient srClient) {
    this.srClient = Objects.requireNonNull(srClient, "srClient");
  }

  @Override
  public void onDeserializationFailure(
      final String source,
      final String changelog,
      final byte[] data
  ) {
    final String sourceSubject = source + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
    final String changelogSubject = changelog + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
    try {
      // all schema registry events start with a magic byte 0x0 and then four bytes
      // indicating the schema id - we extract that schema id from the data that failed
      // to deserialize and then register it into the changelog subject
      final int id = ByteBuffer.wrap(data, 1, Integer.BYTES).getInt();

      LOG.info("Trying to fetch & register schema id {} under subject {}", id, changelogSubject);
      final ParsedSchema schema = srClient.getSchemaBySubjectAndId(sourceSubject, id);
      srClient.register(changelogSubject, schema);
    } catch (IOException | RestClientException e) {
      LOG.warn("Failed during deserialization callback for topic " + source, e);
    }
  }

}
