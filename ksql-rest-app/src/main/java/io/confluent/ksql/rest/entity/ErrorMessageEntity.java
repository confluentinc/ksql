/**
 * Copyright 2017 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("error")
@JsonSubTypes({})
public class ErrorMessageEntity extends KsqlEntity {

  private final ErrorMessage errorMessage;

  @JsonCreator
  public ErrorMessageEntity(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("errorMessage")  ErrorMessage errorMessage
  ) {
    super(statementText);
    this.errorMessage = errorMessage;
  }

  public ErrorMessageEntity(String statementText, Throwable exception) {
    this(statementText, new ErrorMessage(exception));
  }

  public ErrorMessage getErrorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ErrorMessageEntity)) {
      return false;
    }
    ErrorMessageEntity that = (ErrorMessageEntity) o;
    return Objects.equals(getErrorMessage(), that.getErrorMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getErrorMessage());
  }
}
