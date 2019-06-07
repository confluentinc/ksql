/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.Collections;
import java.util.List;

public class KsqlStatementErrorMessage extends KsqlErrorMessage {
  private final String statementText;
  private final KsqlEntityList entities;

  public KsqlStatementErrorMessage(
      @JsonProperty("error_code") final int errorCode,
      @JsonProperty("message") final String message,
      @JsonProperty("stackTrace") final List<String> stackTrace,
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("entities") final KsqlEntityList entities) {
    super(errorCode, message, stackTrace);
    this.entities = entities;
    this.statementText = statementText;
  }

  public KsqlStatementErrorMessage(
      final int errorCode,
      final String message,
      final String statementText,
      final KsqlEntityList entityList) {
    this(errorCode, message, Collections.emptyList(), statementText, entityList);
  }

  public KsqlStatementErrorMessage(
      final int errorCode,
      final Throwable t,
      final String statementText,
      final KsqlEntityList entityList) {
    this(
        errorCode, ErrorMessageUtil.buildErrorMessage(t), getStackTraceStrings(t),
        statementText, entityList);
  }

  public String getStatementText() {
    return statementText;
  }

  public KsqlEntityList getEntities() {
    return entities;
  }
}
