/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution;

import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.physicalplanner.PhysicalPlan;
import io.confluent.ksql.physicalplanner.nodes.Node;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Optional;

/**
 * The {@code ExecutionPlanner} takes a {@link PhysicalPlan} and uses the visitor pattern
 * to translate the logical plan into an execution plan.
 *
 * <p>A execution plan, in contrast to a physical plan, consist of concrete
 * {@link ExecutionStep ExecutionSteps} that describe an executable Kafka Streams
 * {@link org.apache.kafka.streams.Topology}.
 */
public final class ExecutionPlanner {

  private ExecutionPlanner() {}

  public static ExecutionPlan buildPlan(
      final MetaStore metaStore,
      final PhysicalPlan physicalPlan,
      final Sink sink
  ) {
    final PhysicalToExecutionPlanTranslator translator =
        new PhysicalToExecutionPlanTranslator(metaStore);

    final Node<?> planRoot = physicalPlan.getRoot();
    final ExecutionStep<?> root = translator.process(planRoot);

    final FormatInfo keyFormatInfo;
    final FormatInfo valueFormatInfo;
    final SerdeFeatures keyFeatures;
    final SerdeFeatures valueFeatures;

    final CreateSourceAsProperties properties = sink.getProperties();
    final Optional<String> keyFormatName = properties.getKeyFormat();
    if (keyFormatName.isPresent()) {
      keyFormatInfo = FormatInfo.of(keyFormatName.get());
      keyFeatures = SerdeFeatures.of(); // to-do
    } else {
      final Formats formats = planRoot.getFormats();
      keyFormatInfo = formats.getKeyFormat();
      keyFeatures = formats.getKeyFeatures();
    }

    final Optional<String> valueFormatName = properties.getValueFormat();
    if (valueFormatName.isPresent()) {
      valueFormatInfo = FormatInfo.of(valueFormatName.get());
      valueFeatures = SerdeFeatures.of(); // to-do
    } else {
      final Formats formats = planRoot.getFormats();
      valueFormatInfo = formats.getValueFormat();
      valueFeatures = formats.getValueFeatures();
    }

    final Formats outputFormat = Formats.of(
        keyFormatInfo,
        valueFormatInfo,
        keyFeatures,
        valueFeatures
    );

    final StreamSink streamSink = ExecutionStepFactory.streamSink(
        new Stacker().push("OUTPUT"),
        outputFormat,
        (ExecutionStep<KStreamHolder<Object>>) root,
        sink.getName().text(),
        Optional.empty()// timestampColumn
    );

    return new ExecutionPlan(new QueryId("query-id"), streamSink);
  }
}
