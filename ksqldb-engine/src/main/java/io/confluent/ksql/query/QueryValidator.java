/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ExecutionPlan;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;

/**
 * Validate that a given physical plan can be executed by the underlying runtime.
 *
 * <p>Ensures that a given physical plan can be executed by the underlying runtime. It's important
 * to decouple this from query execution, so that a query can be planned and validated, then
 * committed (e.g. to a log of queries to be executed) with the expectation that it should always
 * be able to execute.
 */
public interface QueryValidator {
  void validateTransientQuery(
      SessionConfig config,
      ExecutionPlan executionPlan,
      Collection<QueryMetadata> runningQueries
  );

  void validateQuery(
      SessionConfig config,
      ExecutionPlan executionPlan,
      Collection<QueryMetadata> runningQueries
  );
}
