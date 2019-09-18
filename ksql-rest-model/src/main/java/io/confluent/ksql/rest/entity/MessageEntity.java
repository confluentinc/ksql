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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageEntity extends KsqlEntity {

  private final Optional<String> message;

  public MessageEntity(
      @JsonProperty("statementText")  final String statementText,
      @JsonProperty("warnings")       final List<KsqlWarning> warnings,
      @JsonProperty("message")        final Optional<String> message) {
    super(statementText, warnings);
    Objects.requireNonNull(message);
    this.message = message;
  }

  public Optional<String> getMessage() {
    return message;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MessageEntity that = (MessageEntity) o;
    return message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message);
  }
}

