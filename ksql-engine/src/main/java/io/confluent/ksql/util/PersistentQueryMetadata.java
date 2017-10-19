/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.planner.plan.OutputNode;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Objects;

public class PersistentQueryMetadata extends QueryMetadata {

  private final long id;


  public PersistentQueryMetadata(String statementString, KafkaStreams kafkaStreams,
                                 OutputNode outputNode, String executionPlan, long id,
                                 DataSource.DataSourceType dataSourceType,
                                 String queryApplicationId) {
    super(statementString, kafkaStreams, outputNode, executionPlan, dataSourceType,
          queryApplicationId);
    this.id = id;

  }

  public long getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PersistentQueryMetadata)) {
      return false;
    }

    PersistentQueryMetadata that = (PersistentQueryMetadata) o;

    return Objects.equals(this.id, that.id) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, super.hashCode());
  }
}
