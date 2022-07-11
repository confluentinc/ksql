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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
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
    @JsonSubTypes.Type(value = KafkaTopicsList.class, name = "kafka_topics"),
    @JsonSubTypes.Type(value = KafkaTopicsListExtended.class, name = "kafka_topics_extended"),
    @JsonSubTypes.Type(value = ExecutionPlan.class, name = "executionPlan"),
    @JsonSubTypes.Type(value = SourceDescriptionList.class, name = "source_descriptions"),
    @JsonSubTypes.Type(value = QueryDescriptionList.class, name = "query_descriptions"),
    @JsonSubTypes.Type(value = FunctionDescriptionList.class, name = "describe_function"),
    @JsonSubTypes.Type(value = FunctionNameList.class, name = "function_names"),
    @JsonSubTypes.Type(value = CreateConnectorEntity.class, name = "connector_info"),
    @JsonSubTypes.Type(value = DropConnectorEntity.class, name = "drop_connector"),
    @JsonSubTypes.Type(value = ConnectorList.class, name = "connector_list"),
    @JsonSubTypes.Type(value = ConnectorPluginsList.class, name = "connector_plugins_list"),
    @JsonSubTypes.Type(value = ConnectorDescription.class, name = "connector_description"),
    @JsonSubTypes.Type(value = TypeList.class, name = "type_list"),
    @JsonSubTypes.Type(value = WarningEntity.class, name = "warning_entity"),
    @JsonSubTypes.Type(value = VariablesList.class, name = "variables"),
    @JsonSubTypes.Type(value = TerminateQueryEntity.class, name = "terminate_query"),
    @JsonSubTypes.Type(value = AssertTopicEntity.class, name = "assert_topic"),
    @JsonSubTypes.Type(value = AssertSchemaEntity.class, name = "assert_schema")
})
public abstract class KsqlEntity {
  private final String statementText;
  private final List<KsqlWarning> warnings;

  public KsqlEntity(final String statementText) {
    this(statementText, Collections.emptyList());
  }

  public KsqlEntity(final String statementText, final List<KsqlWarning> warnings) {
    this.statementText = statementText;
    this.warnings = warnings == null ? new ArrayList<>() : new ArrayList<>(warnings);
  }

  public String getStatementText() {
    return statementText;
  }

  public List<KsqlWarning> getWarnings() {
    return Collections.unmodifiableList(warnings);
  }
  
  public void updateWarnings(final List<KsqlWarning> warnings) {
    this.warnings.addAll(warnings);
  }
}
