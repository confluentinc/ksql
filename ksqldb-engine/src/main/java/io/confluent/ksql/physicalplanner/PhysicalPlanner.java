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

import io.confluent.ksql.logicalplanner.LogicalPlan;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physicalplanner.nodes.Node;

/**
 * The {@code PhysicalPlanner} takes a {@link LogicalPlan} and uses the visitor pattern
 * to translate the logical plan into a {@link PhysicalPlan}.
 *
 * <p>A {@link PhysicalPlan}, in contrast to a logical plan, concerns itself with physical schema
 * (ie, column-to-key/value-mapping), data formats, internal data repartitioning etc.
 */
public final class PhysicalPlanner {

  private PhysicalPlanner() {}

  public static PhysicalPlan buildPlan(
      final MetaStore metaStore,
      final LogicalPlan logicalPlan
  ) {
    final LogicalToPhysicalPlanTranslator translator =
        new LogicalToPhysicalPlanTranslator(metaStore);

    final Node<?> root = translator.process(logicalPlan.getRoot());

    return new PhysicalPlan(root);
  }
}
