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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@Immutable
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final List<Expression> projectExpressions;
  private final KeyField keyField;

  @JsonCreator
  public ProjectNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("key") final Optional<String> keyFieldName,
      @JsonProperty("projectExpressions") final List<Expression> projectExpressions
  ) {
    super(id, source.getNodeOutputType());

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");
    this.projectExpressions = requireNonNull(projectExpressions, "projectExpressions");
    this.keyField = KeyField.of(
        requireNonNull(keyFieldName, "keyFieldName"),
        source.getKeyField().legacy())
        .validateKeyExistsIn(schema);

    if (schema.fields().size() != projectExpressions.size()) {
      throw new KsqlException("Error in projection. Schema fields and expression list are not "
          + "compatible.");
    }
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @JsonProperty
  public PlanNode getSource() {
    return source;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  public List<SelectExpression> getProjectSelectExpressions() {
    final List<SelectExpression> selects = new ArrayList<>();
    for (int i = 0; i < projectExpressions.size(); i++) {
      selects.add(SelectExpression.of(schema.fields().get(i).name(), projectExpressions.get(i)));
    }
    return selects;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .select(
            getProjectSelectExpressions(),
            builder.buildNodeContext(getId()),
            builder.getProcessingLogContext()
        );
  }
}
