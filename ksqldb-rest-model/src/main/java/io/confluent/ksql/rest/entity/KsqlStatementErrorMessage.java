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

package io.confluent.ksql.rest.entity;

import static io.confluent.ksql.statement.MaskedStatement.EMPTY_MASKED_STATEMENT;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.statement.MaskedStatement;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KsqlStatementErrorMessage extends KsqlErrorMessage {
  private final MaskedStatement statement;
  private final KsqlEntityList entities;

  @SuppressWarnings("WeakerAccess") // Invoked via reflection
  public KsqlStatementErrorMessage(
      @JsonProperty("error_code") final int errorCode,
      @JsonProperty("message") final String message,
      @JsonProperty("statement") final MaskedStatement statement,
      @JsonProperty("entities") final KsqlEntityList entities) {
    super(errorCode, message);
    this.entities = new KsqlEntityList(entities);
    this.statement = statement;
  }

  public KsqlStatementErrorMessage(
      final int errorCode,
      final Throwable t,
      final String message,
      final KsqlEntityList entityList) {
    this(errorCode, ErrorMessageUtil.buildErrorMessage(t) + message, EMPTY_MASKED_STATEMENT,
        entityList);
  }

  public KsqlStatementErrorMessage(
      final int errorCode,
      final Throwable t,
      final MaskedStatement statement,
      final KsqlEntityList entityList) {
    this(errorCode, ErrorMessageUtil.buildErrorMessage(t), statement, entityList);
  }

  public MaskedStatement getMaskedStatement() {
    return this.statement;
  }

  public KsqlEntityList getEntities() {
    return new KsqlEntityList(entities);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final KsqlStatementErrorMessage that = (KsqlStatementErrorMessage) o;
    return Objects.equals(statement, that.statement)
        && Objects.equals(entities, that.entities)
        && Objects.equals(this.getMessage(), that.getMessage())
        && Objects.equals(this.getErrorCode(), that.getErrorCode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), statement, entities);
  }
}
