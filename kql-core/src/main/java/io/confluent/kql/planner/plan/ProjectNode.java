/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableList;

import io.confluent.kql.parser.tree.Expression;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ProjectNode
    extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final Field keyField;
  private final List<Expression> projectExpressions;

  // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public ProjectNode(@JsonProperty("id") PlanNodeId id,
                     @JsonProperty("source") PlanNode source,
                     @JsonProperty("schema") Schema schema,
                     @JsonProperty("projectExpressions") List<Expression> projectExpressions) {
    super(id);

    requireNonNull(source, "source is null");
    requireNonNull(schema, "schema is null");
    requireNonNull(projectExpressions, "projectExpressions is null");

    this.source = source;
    this.schema = schema;
    this.keyField = source.getKeyField();
    this.projectExpressions = projectExpressions;
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
  public Field getKeyField() {
    return keyField;
  }

  public List<Expression> getProjectExpressions() {
    return projectExpressions;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitProject(this, context);
  }
}
