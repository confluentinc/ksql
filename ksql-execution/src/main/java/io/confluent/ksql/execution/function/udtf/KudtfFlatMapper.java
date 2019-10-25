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

package io.confluent.ksql.execution.function.udtf;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Implements the actual flat-mapping logic - this is called by Kafka Streams
 */
@Immutable
public class KudtfFlatMapper implements ValueMapper<GenericRow, Iterable<GenericRow>> {

  private final TableFunctionApplier functionHolder;

  public KudtfFlatMapper(final TableFunctionApplier functionHolder) {
    this.functionHolder = Objects.requireNonNull(functionHolder);
  }

  @Override
  public Iterable<GenericRow> apply(final GenericRow row) {
    final List<Object> exploded = functionHolder.apply(row);
    final List<GenericRow> rows = new ArrayList<>(exploded.size());
    for (Object val : exploded) {
      final List<Object> arrayList = new ArrayList<>(row.getColumns());
      arrayList.add(val);
      rows.add(new GenericRow(arrayList));
    }
    return rows;
  }
}
