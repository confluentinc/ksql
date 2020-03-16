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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceDescriptionList extends KsqlEntity {

  private final List<SourceDescription> sourceDescriptions;

  @JsonCreator
  public SourceDescriptionList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("sourceDescriptions") final List<SourceDescription> sourceDescriptions,
      @JsonProperty("warnings") final List<KsqlWarning> warnings
  ) {
    super(statementText, warnings);
    this.sourceDescriptions = sourceDescriptions;
  }

  public List<SourceDescription> getSourceDescriptions() {
    return sourceDescriptions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescriptionList)) {
      return false;
    }
    final SourceDescriptionList that = (SourceDescriptionList) o;
    return Objects.equals(sourceDescriptions, that.sourceDescriptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceDescriptions);
  }
}