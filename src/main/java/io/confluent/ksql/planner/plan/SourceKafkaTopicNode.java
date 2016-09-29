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
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.Schema;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
public class SourceKafkaTopicNode
        extends SourceNode
{
    private final String topicName;

    // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
    @JsonCreator
    public SourceKafkaTopicNode(@JsonProperty("id") PlanNodeId id,
                                @JsonProperty("schema") Schema schema,
                                @JsonProperty("topicName") String topicName)
    {
        super(id, schema);

        requireNonNull(topicName, "topicName is null");

        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    @Override
    public List<PlanNode> getSources() {
        return null;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitKafkaTopic(this, context);
    }
}
