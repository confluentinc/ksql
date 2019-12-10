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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamSelectKeyBuilder {

  private static final String EXP_TYPE = "SelectKey";

  private StreamSelectKeyBuilder() {
  }

  public static KStreamHolder<Struct> build(
      final KStreamHolder<?> stream,
      final StreamSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();

    final ExpressionMetadata expression = buildExpressionEvaluator(
        selectKey,
        queryBuilder,
        sourceSchema
    );

    final LogicalSchema resultSchema = new StepSchemaResolver(queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()).resolve(selectKey, sourceSchema);

    final KeyBuilder keyBuilder = StructKeyUtil.keySchema(resultSchema);

    final KStream<?, GenericRow> kstream = stream.getStream();
    final KStream<Struct, GenericRow> rekeyed = kstream
        .filter((key, val) -> val != null && expression.evaluate(val) != null)
        .selectKey((key, val) -> keyBuilder.build(expression.evaluate(val)));

    return new KStreamHolder<>(
        rekeyed,
        resultSchema,
        KeySerdeFactory.unwindowed(queryBuilder)
    );
  }

  private static ExpressionMetadata buildExpressionEvaluator(
      final StreamSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder,
      final LogicalSchema sourceSchema
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(
        sourceSchema,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );

    return codeGen.buildCodeGenFromParseTree(selectKey.getKeyExpression(), EXP_TYPE);
  }
}
