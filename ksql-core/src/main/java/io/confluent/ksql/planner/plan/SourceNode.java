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

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.StructuredDataSource;
import org.apache.kafka.connect.data.Field;

import javax.annotation.concurrent.Immutable;


@Immutable
public abstract class SourceNode extends PlanNode {

  private final StructuredDataSource.DataSourceType dataSourceType;
  private final Field timestampField;

  public SourceNode(@JsonProperty("id") final PlanNodeId id,
                    @JsonProperty("timestampField") final Field timestampField,
                    @JsonProperty("dataSourceType")
                    final StructuredDataSource.DataSourceType dataSourceType) {
    super(id);
    this.dataSourceType = dataSourceType;
    this.timestampField = timestampField;
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public Field getTimestampField() {
    return timestampField;
  }
}
