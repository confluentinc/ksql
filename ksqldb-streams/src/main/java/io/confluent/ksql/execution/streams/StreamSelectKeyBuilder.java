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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.PartitionByParams.Mapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

public final class StreamSelectKeyBuilder {

  private StreamSelectKeyBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamSelectKey<K> selectKey,
      final RuntimeBuildContext buildContext
  ) {
    return build(stream, selectKey, buildContext, PartitionByParamsFactory::build);
  }

  @VisibleForTesting
  static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamSelectKey<K> selectKey,
      final RuntimeBuildContext buildContext,
      final PartitionByParamsBuilder paramsBuilder
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final QueryContext queryContext = selectKey.getProperties().getQueryContext();

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final PartitionByParams<K> params = paramsBuilder.build(
        sourceSchema,
        stream.getExecutionKeyFactory(),
        selectKey.getKeyExpressions(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry(),
        logger
    );

    final Mapper<K> mapper = params.getMapper();

    final KStream<K, GenericRow> kStream = stream.getStream();

    final KStream<K, GenericRow> reKeyed = kStream
        .map(mapper, Named.as(queryContext.formatContext() + "-SelectKey"));

    return new KStreamHolder<>(
        reKeyed,
        params.getSchema(),
        stream.getExecutionKeyFactory().withQueryBuilder(buildContext)
    );
  }

  interface PartitionByParamsBuilder {

    <K> PartitionByParams<K> build(
        LogicalSchema sourceSchema,
        ExecutionKeyFactory<K> executionKeyFactory,
        List<Expression> partitionBy,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger logger
    );
  }
}
