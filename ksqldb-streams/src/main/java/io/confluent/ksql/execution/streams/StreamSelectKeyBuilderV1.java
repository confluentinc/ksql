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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSelectKeyV1;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamSelectKeyBuilderV1 {

  private static final String EXP_TYPE = "SelectKey";

  private StreamSelectKeyBuilderV1() {
  }

  public static KStreamHolder<GenericKey> build(
      final KStreamHolder<?> stream,
      final StreamSelectKeyV1 selectKey,
      final RuntimeBuildContext buildContext
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();

    final CompiledExpression expression = buildExpressionEvaluator(
        selectKey,
        buildContext,
        sourceSchema
    );

    final ProcessingLogger processingLogger = buildContext
        .getProcessingLogger(selectKey.getProperties().getQueryContext());

    final String errorMsg = "Error extracting new key using expression "
        + selectKey.getKeyExpression();

    final Function<GenericRow, Object> evaluator = val -> expression
        .evaluate(val, null, processingLogger, () -> errorMsg);

    final LogicalSchema resultSchema = new StepSchemaResolver(buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()).resolve(selectKey, sourceSchema);

    final KStream<?, GenericRow> kstream = stream.getStream();
    final KStream<GenericKey, GenericRow> rekeyed = kstream
        .filter((key, val) -> val != null && evaluator.apply(val) != null)
        .selectKey((key, val) -> GenericKey.genericKey(evaluator.apply(val)));

    return new KStreamHolder<>(
        rekeyed,
        resultSchema,
        ExecutionKeyFactory.unwindowed(buildContext)
    );
  }

  private static CompiledExpression buildExpressionEvaluator(
      final StreamSelectKeyV1 selectKey,
      final RuntimeBuildContext buildContext,
      final LogicalSchema sourceSchema
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(
        sourceSchema,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    return codeGen.buildCodeGenFromParseTree(selectKey.getKeyExpression(), EXP_TYPE);
  }
}
