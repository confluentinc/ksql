/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.WRAPPER_OBJECT
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommandStatusEntity.class, name = "currentStatus"),
    @JsonSubTypes.Type(value = ErrorMessageEntity.class, name = "error"),
    @JsonSubTypes.Type(value = PropertiesList.class, name = "properties"),
    @JsonSubTypes.Type(value = Queries.class, name = "queries"),
    @JsonSubTypes.Type(value = SourceDescription.class, name = "description"),
    @JsonSubTypes.Type(value = TopicDescription.class, name = "topic_description"),
    @JsonSubTypes.Type(value = StreamsList.class, name = "streams"),
    @JsonSubTypes.Type(value = TablesList.class, name = "tables"),
    @JsonSubTypes.Type(value = KsqlTopicsList.class, name = "topics"),
    @JsonSubTypes.Type(value = ExecutionPlan.class, name = "executionPlan")
})
public abstract class KsqlEntity {
  private final String statementText;

  public KsqlEntity(String statementText) {
    this.statementText = statementText;
  }

  public String getStatementText() {
    return statementText;
  }
}
