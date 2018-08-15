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
import java.util.Objects;


public class TopicDescription extends KsqlEntity {
  private final String name;
  private final String kafkaTopic;
  private final String format;
  private final String schemaString;


  @JsonCreator
  public TopicDescription(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("name") final String name,
      @JsonProperty("topic") final String kafkaTopic,
      @JsonProperty("format") final String format,
      @JsonProperty("schemaString") final String schemaString
  ) {
    super(statementText);
    this.name = name;
    this.kafkaTopic = kafkaTopic;
    this.format = format;
    this.schemaString = schemaString;
  }

  public String getName() {
    return name;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getFormat() {
    return format;
  }

  public String getSchemaString() {
    return schemaString;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicDescription)) {
      return false;
    }
    final TopicDescription that = (TopicDescription) o;
    return Objects.equals(getName(), that.getName())
           && Objects.equals(getKafkaTopic(), that.getKafkaTopic())
           && Objects.equals(getFormat(), that.getFormat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getKafkaTopic(), getFormat());
  }
}
