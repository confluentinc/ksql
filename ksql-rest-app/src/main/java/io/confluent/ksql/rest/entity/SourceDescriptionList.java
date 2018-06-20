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

import java.util.List;
import java.util.Objects;

public class SourceDescriptionList extends KsqlEntity {

  private final List<SourceDescription> sourceDescriptions;

  @JsonCreator
  public SourceDescriptionList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("sourceDescriptions") List<SourceDescription> sourceDescriptions
  ) {
    super(statementText);
    this.sourceDescriptions = sourceDescriptions;
  }

  public List<SourceDescription> getSourceDescriptions() {
    return sourceDescriptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescriptionList)) {
      return false;
    }
    SourceDescriptionList that = (SourceDescriptionList) o;
    return Objects.equals(sourceDescriptions, that.sourceDescriptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceDescriptions);
  }
}