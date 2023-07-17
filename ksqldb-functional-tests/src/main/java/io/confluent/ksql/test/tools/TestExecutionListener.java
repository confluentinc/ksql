/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.SourceNode;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;

public interface TestExecutionListener {

  default void acceptPlan(final ConfiguredKsqlPlan plan) {
  }

  default void acceptQuery(final PersistentQueryMetadata query) {
  }

  default void runComplete(final List<PostTopicNode> knownTopics, List<SourceNode> knownSources) {
  }

  static TestExecutionListener noOp() {
    return new TestExecutionListener() {
    };
  }
}
