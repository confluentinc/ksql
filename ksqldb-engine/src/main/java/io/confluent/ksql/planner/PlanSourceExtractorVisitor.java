/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner;

import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import java.util.stream.Stream;

public class PlanSourceExtractorVisitor {

  public PlanSourceExtractorVisitor() {
  }

  public Stream<DataSource> extract(final PlanNode node) {
    if (node instanceof DataSourceNode) {
      return Stream.of(((DataSourceNode) node).getDataSource());
    }

    return node.getSources().stream()
        .flatMap(this::extract)
        .distinct();
  }
}
