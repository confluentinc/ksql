/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericKey.Builder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.process.KsProcessor;
import io.confluent.ksql.execution.streams.process.KsFixedKeyProcessor;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.Selection;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Named;


public final class StreamSelectBuilder {
  private StreamSelectBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> streamHolder,
      final StreamSelect<K> step,
      final RuntimeBuildContext buildContext
  ) {
    final QueryContext queryContext = step.getProperties().getQueryContext();

    final LogicalSchema sourceSchema = streamHolder.getSchema();
    final Optional<ImmutableList<ColumnName>> selectedKeys = step.getSelectedKeys();

    final Selection<K> selection = Selection.of(
        sourceSchema,
        step.getKeyColumnNames(),
        selectedKeys,
        step.getSelectExpressions(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final ImmutableList.Builder<Integer> keyIndexBuilder = ImmutableList.builder();
    if (selectedKeys.isPresent()) {
      final ImmutableList<ColumnName> keyNames = sourceSchema.key().stream()
          .map(Column::name)
          .collect(ImmutableList.toImmutableList());

      for (final ColumnName keyName : selectedKeys.get()) {
        keyIndexBuilder.add(keyNames.indexOf(keyName));
      }
    }
    final ImmutableList<Integer> keyIndices = keyIndexBuilder.build();

    final SelectValueMapper<K> selectMapper = selection.getMapper();

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final Named selectName =
        Named.as(StreamsUtil.buildOpName(queryContext));

    if (selectedKeys.isPresent() && !selectedKeys.get().containsAll(
        sourceSchema.key().stream().map(Column::name).collect(ImmutableList.toImmutableList())
    )) {
      return streamHolder.withStream(
          streamHolder.getStream().process(() -> new KsProcessor<>(
              (readOnlyKey, value, ctx) -> {
                if (keyIndices.isEmpty()) {
                  return null;
                }

                if (readOnlyKey instanceof GenericKey) {
                  final GenericKey keys = (GenericKey) readOnlyKey;
                  final Builder resultKeys = GenericKey.builder(keyIndices.size());

                  for (final int keyIndex : keyIndices) {
                    resultKeys.append(keys.get(keyIndex));
                  }

                  return (K) resultKeys.build();
                } else {
                  throw new UnsupportedOperationException();
                }
              },
              selectMapper.getTransformer(logger)), selectName),
          selection.getSchema()
      );
    } else {
      return streamHolder.withStream(
          streamHolder.getStream().processValues(
              () -> new KsFixedKeyProcessor<>(selectMapper.getTransformer(logger)), selectName),
          selection.getSchema()
      );
    }
  }
}
