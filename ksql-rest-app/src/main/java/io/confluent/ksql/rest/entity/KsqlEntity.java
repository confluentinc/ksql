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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommandStatusEntity.class, name = "currentStatus"),
    @JsonSubTypes.Type(value = PropertiesList.class, name = "properties"),
    @JsonSubTypes.Type(value = Queries.class, name = "queries"),
    @JsonSubTypes.Type(value = SourceDescriptionEntity.class, name = "sourceDescription"),
    @JsonSubTypes.Type(value = QueryDescriptionEntity.class, name = "queryDescription"),
    @JsonSubTypes.Type(value = TopicDescription.class, name = "topicDescription"),
    @JsonSubTypes.Type(value = StreamsList.class, name = "streams"),
    @JsonSubTypes.Type(value = TablesList.class, name = "tables"),
    @JsonSubTypes.Type(value = KsqlTopicsList.class, name = "ksql_topics"),
    @JsonSubTypes.Type(value = KafkaTopicsList.class, name = "kafka_topics"),
    @JsonSubTypes.Type(value = ExecutionPlan.class, name = "executionPlan"),
    @JsonSubTypes.Type(value = SourceDescriptionList.class, name = "source_descriptions"),
    @JsonSubTypes.Type(value = QueryDescriptionList.class, name = "query_descriptions"),
    @JsonSubTypes.Type(value = FunctionDescriptionList.class, name = "describe_function"),
    @JsonSubTypes.Type(value = FunctionNameList.class, name = "function_names")
})
public abstract class KsqlEntity {
  private final String statementText;

  public KsqlEntity(final String statementText) {
    this.statementText = statementText;
  }

  public String getStatementText() {
    return statementText;
  }
}
