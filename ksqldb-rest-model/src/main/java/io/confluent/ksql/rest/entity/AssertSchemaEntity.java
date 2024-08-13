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
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AssertSchemaEntity extends KsqlEntity {
  private final Optional<String> subject;
  private final Optional<Integer> id;
  private final boolean exists;

  public AssertSchemaEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("subject") final Optional<String> subject,
      @JsonProperty("id") final Optional<Integer> id,
      @JsonProperty("exists") final boolean exists
  ) {
    super(statementText);
    this.subject = Objects.requireNonNull(subject, "subject");
    this.id = Objects.requireNonNull(id, "id");
    this.exists = exists;
  }

  public Optional<String> getSubject() {
    return subject;
  }

  public Optional<Integer> getId() {
    return id;
  }

  public boolean getExists() {
    return exists;
  }

  @Override
  public String toString() {
    return "AssertSchemaEntity{"
        + "subject='" + subject + '\''
        + ", id=" + id
        + ", exists=" + exists
        + '}';
  }
}
