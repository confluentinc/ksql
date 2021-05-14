/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.services.KafkaTopicClient;
import java.util.List;
import java.util.Optional;

/**
 * Abstract base class for logical plan nodes with only a single source
 */
public abstract class SingleSourcePlanNode extends PlanNode {

  private final PlanNode source;

  public SingleSourcePlanNode(
      final PlanNodeId id,
      final DataSourceType nodeOutputType,
      final Optional<SourceName> sourceName,
      final PlanNode source
  ) {
    super(id, nodeOutputType, sourceName);

    this.source = requireNonNull(source, "source");
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }
}
