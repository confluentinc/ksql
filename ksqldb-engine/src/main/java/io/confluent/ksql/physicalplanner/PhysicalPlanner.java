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

package io.confluent.ksql.physicalplanner;

import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.logicalplanner.LogicalPlan;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Collections;
import java.util.Optional;

/**
 * The {@code PhysicalPlanner} takes a {@link LogicalPlan} and uses the visitor pattern
 * to translate the logical plan into a physical plan.
 *
 * <p>A physical plan, in contrast to a logical plan, concerns itself with physical schema
 * (ie, column-to-key/value-mapping), data formats, internal data repartitioning etc.
 */
public final class PhysicalPlanner {

  private PhysicalPlanner() {}

  public static PhysicalPlan buildPhysicalPlan(
      final MetaStore metaStore,
      final LogicalPlan logicalPlan
  ) {
    final LogicalToPhysicalPlanTranslator translator =
        new LogicalToPhysicalPlanTranslator(metaStore);

    final ExecutionStep<?> root = translator.process(logicalPlan.getRoot());

    final Formats formats = Formats.of(
        FormatInfo.of("KAFKA"),
        FormatInfo.of("JSON"),
        SerdeFeatures.from(Collections.emptySet()),
        SerdeFeatures.from(Collections.emptySet())
    );

    final StreamSink sink = ExecutionStepFactory.streamSink(
        new Stacker().push("OUTPUT"),
        formats,
        (ExecutionStep<KStreamHolder<Object>>) root,
        "OUTPUT",
        Optional.empty()// timestampColumn
    );

    return new PhysicalPlan(new QueryId("query-id"), sink);
  }
}
