/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.execution.streams.transform.KsTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.Selection;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.kstream.Named;

public final class StreamMapValuesBuilder {
  private StreamMapValuesBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamMapValues<K> step,
      final KsqlQueryBuilder queryBuilder
  ) {
    final QueryContext.Stacker contextStacker = QueryContext.Stacker.of(
        step.getProperties().getQueryContext()
    );

    final LogicalSchema sourceSchema = step.getSource().getProperties().getSchema();

    final Selection<K> selection = Selection.of(
        sourceSchema,
        step.getSelectExpressions(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );

    final SelectValueMapper<K> selectMapper = selection.getMapper();

    final ProcessingLogger logger = queryBuilder
        .getProcessingLogContext()
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                queryBuilder.getQueryId(),
                contextStacker.push("PROJECT").getQueryContext()
            )
        );

    final Named selectName = Named.as(queryBuilder.buildUniqueNodeName(step.getSelectNodeName()));

    return stream.withStream(
        stream.getStream().transformValues(
            () -> new KsTransformer<>(selectMapper.getTransformer(logger)),
            selectName
        ),
        selection.getSchema()
    );
  }
}
