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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.rest.server.resources.Errors;

import java.util.Collections;
import java.util.List;

public class KsqlQueryEndpointMessage extends KsqlStatementErrorMessage {
  @JsonCreator
  public KsqlQueryEndpointMessage(
      @JsonProperty("error_code") int errorCode,
      @JsonProperty("message") String message,
      @JsonProperty("stackTrace") List<String> stackTrace,
      @JsonProperty("statementText") String statementText,
      @JsonProperty("entities") KsqlEntityList entitities
  ) {
    super(errorCode, message, stackTrace, statementText, entitities);
  }

  public KsqlQueryEndpointMessage(String statementText, KsqlEntityList entities) {
    super(
        Errors.ERROR_CODE_QUERY_ENDPOINT, "SELECT queries must use the /query endpoint",
        Collections.emptyList(), statementText, entities);
  }
}
