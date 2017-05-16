package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.WRAPPER_OBJECT
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommandIdEntity.class, name = "command_id"),
    @JsonSubTypes.Type(value = KSQLError.class, name = "error"),
    @JsonSubTypes.Type(value = PropertiesList.class, name = "properties"),
    @JsonSubTypes.Type(value = RunningQueries.class, name = "running_queries"),
    @JsonSubTypes.Type(value = SetProperty.class, name = "set_property"),
    @JsonSubTypes.Type(value = SourceDescription.class, name = "description"),
    @JsonSubTypes.Type(value = StreamsList.class, name = "streams"),
    @JsonSubTypes.Type(value = TablesList.class, name = "tables"),
    @JsonSubTypes.Type(value = TopicsList.class, name = "topics")
})
public abstract class KSQLEntity {
  private final String statementText;

  public KSQLEntity(String statementText) {
    this.statementText = statementText;
  }

  public String getStatementText() {
    return statementText;
  }
}
