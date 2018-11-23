/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.structured;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ExpressionMetadata;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupByMapper<K> implements KeyValueMapper<K, GenericRow, String> {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByMapper.class);

  private static final String GROUP_BY_COLUMN_SEPARATOR = "|+|";

  private final List<ExpressionMetadata> expressions;

  GroupByMapper(final List<ExpressionMetadata> expressions) {
    this.expressions = ImmutableList.copyOf(Objects.requireNonNull(expressions, "expressions"));
    if (expressions.isEmpty()) {
      throw new IllegalArgumentException("Empty group by");
    }
  }

  @Override
  public String apply(final K key, final GenericRow row) {
    return IntStream.range(0, expressions.size())
        .mapToObj(idx -> processColumn(idx, expressions.get(idx), row))
        .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR));
  }

  static String keyNameFor(final List<Expression> groupByExpressions) {
    return groupByExpressions.stream()
        .map(Expression::toString)
        .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR));
  }

  private String processColumn(
      final int index,
      final ExpressionMetadata exp,
      final GenericRow row
  ) {
    try {
      return String.valueOf(exp.evaluate(row));
    } catch (final Exception e) {
      LOG.error("Error calculating group-by field with index {}", index, e);
      return "null";
    }
  }
}
