/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import java.util.List;

public interface ExecutionStep<S> {
  ExecutionStepProperties getProperties();

  List<ExecutionStep<?>> getSources();

  S build(KsqlQueryBuilder queryBuilder);
}
