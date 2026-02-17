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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implements the actual flat-mapping logic - this is called by Kafka Streams
 */
@Immutable
public class KudtfFlatMapper<K> implements KsqlTransformer<K, Iterable<GenericRow>> {

  @EffectivelyImmutable
  private final ProcessingLogger processingLogger;
  private final ImmutableList<TableFunctionApplier> tableFunctionAppliers;

  public KudtfFlatMapper(
      final List<TableFunctionApplier> tableFunctionAppliers,
      final ProcessingLogger processingLogger
  ) {
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    this.tableFunctionAppliers = ImmutableList
        .copyOf(requireNonNull(tableFunctionAppliers, "tableFunctionAppliers"));
  }

  /*
  This function zips results from multiple table functions together as described in KLIP-9
  in the design-proposals directory.
   */

  @Override
  public Iterable<GenericRow> transform(
      final K readOnlyKey,
      final GenericRow value
  ) {
    if (value == null) {
      return null;
    }

    final List<Iterator<?>> iters = new ArrayList<>(tableFunctionAppliers.size());
    int maxLength = 0;
    for (final TableFunctionApplier applier : tableFunctionAppliers) {
      final List<?> exploded = applier.apply(value, processingLogger);
      iters.add(exploded.iterator());
      maxLength = Math.max(maxLength, exploded.size());
    }

    final List<GenericRow> rows = new ArrayList<>(maxLength);
    for (int i = 0; i < maxLength; i++) {
      final GenericRow newRow = new GenericRow(value.values().size() + iters.size());
      newRow.appendAll(value.values());

      for (final Iterator<?> iter : iters) {
        if (iter.hasNext()) {
          newRow.append(iter.next());
        } else {
          newRow.append(null);
        }
      }
      rows.add(newRow);
    }
    return rows;
  }
}
