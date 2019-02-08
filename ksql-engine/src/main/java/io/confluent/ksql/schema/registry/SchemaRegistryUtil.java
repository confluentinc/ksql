/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.schema.registry;

import static io.confluent.ksql.util.ExecutorUtil.RetryBehaviour.ALWAYS;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaRegistryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryUtil.class);

  private static final String CHANGE_LOG_SUFFIX = KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX
      + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private static final String REPARTITION_SUFFIX = KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX
      + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private SchemaRegistryUtil() {
  }

  public static void cleanUpInternalTopicAvroSchemas(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    getInternalSubjectNameSet(applicationId, schemaRegistryClient)
        .forEach(subject -> tryDeleteSubject(applicationId, schemaRegistryClient, subject));
  }

  public static void maybeCleanUpSourceTopicAvroSchema(
      final StructuredDataSource dataSource, final SchemaRegistryClient schemaRegistryClient) {
    if (dataSource.getKsqlTopicSerde().getSerDe()
        == DataSource.DataSourceSerDe.AVRO) {
      final String sourceName = dataSource.getName();
      try {
        final String subject = sourceName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

        ExecutorUtil.executeWithRetries(() -> schemaRegistryClient.deleteSubject(subject), ALWAYS);
      } catch (final Exception e) {
        throw new KsqlException("Could not clean up the schema registry for topic: "
            + sourceName, e);
      }
    }
  }

  private static Stream<String> getInternalSubjectNameSet(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    try {

      return schemaRegistryClient.getAllSubjects().stream()
          .filter(subjectName -> subjectName.startsWith(applicationId))
          .filter(subjectName ->
              subjectName.endsWith(CHANGE_LOG_SUFFIX) || subjectName.endsWith(REPARTITION_SUFFIX));

    } catch (final Exception e) {
      // Do nothing! Schema registry clean up is best effort!
      LOG.warn("Could not clean up the schema registry for query: " + applicationId, e);
      return Stream.empty();
    }
  }

  private static void tryDeleteSubject(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient,
      final String subjectName
  ) {
    try {
      schemaRegistryClient.deleteSubject(subjectName);
    } catch (final Exception e) {
      LOG.warn("Could not clean up the schema registry for"
          + " query: " + applicationId
          + ", subject: " + subjectName, e);
    }
  }
}