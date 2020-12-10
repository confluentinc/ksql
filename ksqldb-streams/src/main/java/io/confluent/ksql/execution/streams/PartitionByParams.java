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

package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public final class PartitionByParams<K> {

  private final LogicalSchema schema;
  private final Mapper<K> mapper;

  public PartitionByParams(
      final LogicalSchema schema,
      final Mapper<K> mapper
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.mapper = requireNonNull(mapper, "mapper");
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public Mapper<K> getMapper() {
    return mapper;
  }

  public interface Mapper<K> extends KeyValueMapper<K, GenericRow, KeyValue<K, GenericRow>> { }

}
