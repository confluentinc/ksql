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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

public final class StreamSelectKeyBuilder {

  private StreamSelectKeyBuilder() {
  }

  public static KStreamHolder<Struct> build(
      final KStreamHolder<?> stream,
      final StreamSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder
  ) {
    return build(stream, selectKey, queryBuilder, PartitionByParamsFactory::build);
  }

  @VisibleForTesting
  static KStreamHolder<Struct> build(
      final KStreamHolder<?> stream,
      final StreamSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder,
      final PartitionByParamsBuilder paramsBuilder
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final QueryContext queryContext = selectKey.getProperties().getQueryContext();

    final ProcessingLogger logger = queryBuilder.getProcessingLogger(queryContext);

    final PartitionByParams params = paramsBuilder.build(
        sourceSchema,
        selectKey.getKeyExpression(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        logger
    );

    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper = params.getMapper();

    final KStream<?, GenericRow> kStream = stream.getStream();

    final KStream<Struct, GenericRow> reKeyed = kStream
        .map(mapper::apply, Named.as(queryContext.formatContext() + "-SelectKey"));

    return new KStreamHolder<>(
        reKeyed,
        params.getSchema(),
        KeySerdeFactory.unwindowed(queryBuilder)
    );
  }

  interface PartitionByParamsBuilder {

    PartitionByParams build(
        LogicalSchema sourceSchema,
        Expression partitionBy,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger logger
    );
  }
}
