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
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RegisterSchemaCallback implements StaticTopicSerde.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterSchemaCallback.class);
  private final SchemaRegistryClient srClient;
  private final Set<SchemaRegisterEvent> failedAttempts = new HashSet<>();

  RegisterSchemaCallback(final SchemaRegistryClient srClient) {
    this.srClient = Objects.requireNonNull(srClient, "srClient");
  }

  @Override
  public void onDeserializationFailure(
      final String source,
      final String changelog,
      final byte[] data
  ) {
    // NOTE: this only happens for values, we should never auto-register key schemas
    final String sourceSubject = KsqlConstants.getSRSubject(source, false);
    final String changelogSubject = KsqlConstants.getSRSubject(changelog, false);

    // all schema registry events start with a magic byte 0x0 and then four bytes
    // indicating the schema id - we extract that schema id from the data that failed
    // to deserialize and then register it into the changelog subject
    final int id = ByteBuffer.wrap(data, 1, Integer.BYTES).getInt();
    final SchemaRegisterEvent event = new SchemaRegisterEvent(id, sourceSubject, changelogSubject);

    try {
      if (!failedAttempts.contains(event)) {
        LOG.info("Trying to fetch & register schema id {} under subject {}", id, changelogSubject);
        final ParsedSchema schema = srClient.getSchemaBySubjectAndId(sourceSubject, id);
        srClient.register(changelogSubject, schema);
      }
    } catch (Exception e) {
      LOG.warn("Failed during deserialization callback for topic {}. "
          + "Will not try again to register id {} under subject {}.",
          source,
          id,
          changelogSubject,
          e
      );

      failedAttempts.add(event);
    }
  }

  private static final class SchemaRegisterEvent {
    final int id;
    final String sourceSubject;
    final String changelogSubject;

    private SchemaRegisterEvent(
        final int id,
        final String sourceSubject,
        final String changelogSubject
    ) {
      this.id = id;
      this.sourceSubject = sourceSubject;
      this.changelogSubject = changelogSubject;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final SchemaRegisterEvent that = (SchemaRegisterEvent) o;
      return id == that.id
          && Objects.equals(sourceSubject, that.sourceSubject)
          && Objects.equals(changelogSubject, that.changelogSubject);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, sourceSubject, changelogSubject);
    }
  }

}
