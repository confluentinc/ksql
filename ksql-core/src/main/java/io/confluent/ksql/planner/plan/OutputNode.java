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
package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.planner.KSQLSchema;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class OutputNode
        extends PlanNode
{
    private final PlanNode source;
    private final KSQLSchema schema;

    @JsonCreator
    protected OutputNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("source") PlanNode source,
                      @JsonProperty("schema")KSQLSchema schema)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(schema, "schema is null");

        this.source = source;
        this.schema = schema;
    }

    @Override
    public KSQLSchema getSchema() {
        return this.schema;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }
}
