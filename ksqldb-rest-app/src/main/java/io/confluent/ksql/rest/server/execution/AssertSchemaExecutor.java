/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.util.KsqlConfig.KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.AssertSchemaEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class AssertSchemaExecutor {

  private static final int HTTP_NOT_FOUND = 404;
  private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  private AssertSchemaExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<AssertSchema> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return AssertExecutor.execute(
        statement.getMaskedStatementText(),
        statement.getStatement(),
        executionContext.getKsqlConfig().getInt(KSQL_ASSERT_SCHEMA_DEFAULT_TIMEOUT_MS),
        serviceContext,
        (stmt, sc) -> assertSchema(
            sc.getSchemaRegistryClient(),
            ((AssertSchema) stmt).getSubject(),
            ((AssertSchema) stmt).getId(),
            stmt.checkExists()),
        (str, stmt) -> new AssertSchemaEntity(
            str,
            ((AssertSchema) stmt).getSubject(),
            ((AssertSchema) stmt).getId(),
            stmt.checkExists())
    );
  }

  private static void assertSchema(
      final SchemaRegistryClient client,
      final Optional<String> subject,
      final Optional<Integer> id,
      final boolean assertExists
  ) {
    final boolean schemaExists = checkSchemaExistence(client, subject, id);
    final String subjectString = subject.isPresent() ? " subject name " + subject.get() : "";
    final String idString = id.isPresent() ? " id " + id.get() : "";
    if (!assertExists) {
      if (schemaExists) {
        throw new KsqlException("Schema with" + subjectString + idString + " exists");
      }
    } else {
      if (!schemaExists) {
        throw new KsqlException("Schema with" + subjectString + idString + " does not exist");
      }
    }
  }

  private static boolean checkSchemaExistence(
      final SchemaRegistryClient client,
      final Optional<String> subject,
      final Optional<Integer> id
  ) {
    try {
      if (subject.isPresent() && id.isPresent()) {
        return client.getAllSubjectsById(id.get()).contains(subject.get());
      } else if (id.isPresent()) {
        client.getSchemaById(id.get());
      } else if (subject.isPresent()) {
        client.getLatestSchemaMetadata(subject.get());
      }
      return true;
    } catch (final Exception e) {
      if (isSchemaNotFoundException(e)) {
        return false;
      } else {
        throw new KsqlException("Cannot check schema existence: " + e.getMessage());
      }
    }
  }

  private static boolean isSchemaNotFoundException(final Exception e) {
    if (e instanceof RestClientException) {
      return ((RestClientException) e).getStatus() == HTTP_NOT_FOUND
          && (((RestClientException) e).getErrorCode() == SCHEMA_NOT_FOUND_ERROR_CODE
          || ((RestClientException) e).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE);
    }
    return false;
  }
}
