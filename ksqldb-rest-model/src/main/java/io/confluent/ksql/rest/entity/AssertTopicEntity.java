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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AssertTopicEntity extends KsqlEntity {
  private final String topicName;
  private final boolean exists;

  public AssertTopicEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("topicName") final String topicName,
      @JsonProperty("exists") final boolean exists
  ) {
    super(statementText);
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.exists = exists;
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean getExists() {
    return exists;
  }

  @Override
  public String toString() {
    return "AssertTopicEntity{"
        + "topicName='" + topicName + '\''
        + ", exists=" + exists
        + '}';
  }
}
