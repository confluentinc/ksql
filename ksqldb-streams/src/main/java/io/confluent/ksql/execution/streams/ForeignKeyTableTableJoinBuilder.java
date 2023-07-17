/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KTable;

public final class ForeignKeyTableTableJoinBuilder {

  private ForeignKeyTableTableJoinBuilder() {
  }

  public static <KLeftT, KRightT> KTableHolder<KLeftT> build(
      final KTableHolder<KLeftT> left,
      final KTableHolder<KRightT> right,
      final ForeignKeyTableTableJoin<KLeftT, KRightT> join,
      final RuntimeBuildContext buildContext
  ) {
    final LogicalSchema leftSchema = left.getSchema();
    final LogicalSchema rightSchema = right.getSchema();

    final ProcessingLogger logger = buildContext.getProcessingLogger(
        join.getProperties().getQueryContext()
    );

    final ExpressionEvaluator expressionEvaluator;
    final CodeGenRunner codeGenRunner = new CodeGenRunner(
        leftSchema,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final Optional<ColumnName> leftColumnName = join.getLeftJoinColumnName();
    final Optional<Expression> leftJoinExpression = join.getLeftJoinExpression();
    if (leftColumnName.isPresent()) {
      expressionEvaluator = codeGenRunner.buildCodeGenFromParseTree(
          new UnqualifiedColumnReferenceExp(leftColumnName.get()),
          "Left Join Expression"
      );
    } else if (leftJoinExpression.isPresent()) {
      expressionEvaluator = codeGenRunner.buildCodeGenFromParseTree(
          leftJoinExpression.get(),
          "Left Join Expression"
      );
    } else {
      throw new IllegalStateException("Both leftColumnName and leftJoinExpression are empty.");
    }

    final ForeignKeyJoinParams<KRightT> joinParams = ForeignKeyJoinParamsFactory
        .create(expressionEvaluator, leftSchema, rightSchema, logger);

    final Formats formats = join.getFormats();

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        joinParams.getSchema(),
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final Serde<KLeftT> keySerde = left.getExecutionKeyFactory().buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        join.getProperties().getQueryContext()
    );
    final Serde<GenericRow> valSerde = buildContext.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        join.getProperties().getQueryContext()
    );

    final KTable<KLeftT, GenericRow> result;
    switch (join.getJoinType()) {
      case INNER:
        result = left.getTable().join(
            right.getTable(),
            joinParams.getKeyExtractor(),
            joinParams.getJoiner(),
            buildContext.getMaterializedFactory().create(keySerde, valSerde)
        );
        break;
      case LEFT:
        result = left.getTable().leftJoin(
            right.getTable(),
            joinParams.getKeyExtractor(),
            joinParams.getJoiner(),
            buildContext.getMaterializedFactory().create(keySerde, valSerde)
        );
        break;
      default:
        throw new IllegalStateException("invalid join type: " + join.getJoinType());
    }

    return KTableHolder.unmaterialized(
            result,
            joinParams.getSchema(),
            left.getExecutionKeyFactory()
    );
  }
}