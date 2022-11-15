/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.scalablepush;

import io.confluent.ksql.util.PushOffsetRange;
import java.util.Optional;

/**
 * Creates a physical plan, given a logical plan that's already known.
 */
public interface PushPhysicalPlanCreator {

  /**
   * Creates the physical plan
   * @param offsetRange The optional offset range to use to start the plan from.
   * @param catchupConsumerGroup The consumer group to use for catchup operations, if provided
   * @return The created, ready to execute plan.
   */
  PushPhysicalPlan create(Optional<PushOffsetRange> offsetRange,
      Optional<String> catchupConsumerGroup);
}
