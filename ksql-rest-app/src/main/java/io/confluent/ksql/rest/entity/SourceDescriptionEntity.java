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

import java.util.Objects;

public class SourceDescriptionEntity extends KsqlEntity {
  private final SourceDescription sourceDescription;

  @JsonCreator
  public SourceDescriptionEntity(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("sourceDescription") SourceDescription sourceDescription) {
    super(statementText);
    this.sourceDescription = sourceDescription;
  }

  public SourceDescription getSourceDescription() {
    return sourceDescription;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescriptionEntity)) {
      return false;
    }
    SourceDescriptionEntity other = (SourceDescriptionEntity)o;
    return Objects.equals(sourceDescription, other.sourceDescription);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSourceDescription());
  }
}
