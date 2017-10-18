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

package io.confluent.ksql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.StructuredDataSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class StructuredDataSourceNode
    extends PlanNode {

  private final StructuredDataSource structuredDataSource;
  private final Schema schema;

  // TODO: pass in the "assignments" and the "outputs" separately
  // TODO: (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public StructuredDataSourceNode(@JsonProperty("id") final PlanNodeId id,
                                  @JsonProperty("structuredDataSource") final StructuredDataSource structuredDataSource,
                                  @JsonProperty("schema") Schema schema) {
    super(id);
    Objects.requireNonNull(structuredDataSource, "structuredDataSource can't be null");
    Objects.requireNonNull(schema, "schema can't be null");
    this.schema = schema;
    this.structuredDataSource = structuredDataSource;
  }

  public String getTopicName() {
    return structuredDataSource.getTopicName();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Field getKeyField() {
    return structuredDataSource.getKeyField();
  }

  public StructuredDataSource getStructuredDataSource() {
    return structuredDataSource;
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitStructuredDataSourceNode(this, context);
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return structuredDataSource.getDataSourceType();
  }

  public Field getTimestampField() {
    return structuredDataSource.getTimestampField();
  }
}
