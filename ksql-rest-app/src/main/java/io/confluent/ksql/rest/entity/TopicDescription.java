/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

import io.confluent.ksql.metastore.StructuredDataSource;

@JsonTypeName("topic_description")
public class TopicDescription extends KsqlEntity {
  private final String name;
  private final String kafkaTopic;
  private final String format;
  private final String schemaString;


  @JsonCreator
  public TopicDescription(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("name")          String name,
      @JsonProperty("kafkaTopic")    String kafkaTopic,
      @JsonProperty("format")        String format,
      @JsonProperty("schemaString")  String schemaString
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicDescription)) {
      return false;
    }
    TopicDescription that = (TopicDescription) o;
    return Objects.equals(getName(), that.getName())
           && Objects.equals(getKafkaTopic(), that.getKafkaTopic())
           && Objects.equals(getFormat(), that.getFormat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getKafkaTopic(), getFormat());
  }
}
