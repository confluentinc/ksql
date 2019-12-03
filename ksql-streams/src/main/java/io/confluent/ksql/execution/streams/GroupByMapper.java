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

package io.confluent.ksql.execution.streams;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.util.StructKeyUtil;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GroupByMapper<K> implements KeyValueMapper<K, GenericRow, Struct> {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByMapper.class);

  private static final String GROUP_BY_VALUE_SEPARATOR = "|+|";

  private final List<ExpressionMetadata> expressions;

  GroupByMapper(final List<ExpressionMetadata> expressions) {
    this.expressions = ImmutableList.copyOf(Objects.requireNonNull(expressions, "expressions"));
    if (expressions.isEmpty()) {
      throw new IllegalArgumentException("Empty group by");
    }
  }

  @Override
  public Struct apply(final K readOnlyKey, final GenericRow value) {
    final String stringRowKey = IntStream.range(0, expressions.size())
        .mapToObj(idx -> processColumn(idx, expressions.get(idx), readOnlyKey, value))
        .collect(Collectors.joining(GROUP_BY_VALUE_SEPARATOR));

    return StructKeyUtil.asStructKey(stringRowKey);
  }

  private static String processColumn(
      final int index,
      final ExpressionMetadata exp,
      final Object readOnlyKey,
      final GenericRow value
  ) {
    try {
      return String.valueOf(exp.evaluate(readOnlyKey, value));
    } catch (final Exception e) {
      LOG.error("Error calculating group-by field with index {}", index, e);
      return "null";
    }
  }

  List<ExpressionMetadata> getExpressionMetadata() {
    return expressions;
  }
}
