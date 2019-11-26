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

package io.confluent.ksql.structured;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SchemaKGroupedStream {

  final ExecutionStep<KGroupedStreamHolder> sourceStep;
  final KeyFormat keyFormat;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;

  SchemaKGroupedStream(
      final ExecutionStep<KGroupedStreamHolder> sourceStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.sourceStep = sourceStep;
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = functionRegistry;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public ExecutionStep<KGroupedStreamHolder> getSourceStep() {
    return sourceStep;
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<?> aggregate(
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final ExecutionStep<? extends KTableHolder<?>> step;
    final KeyFormat keyFormat;

    if (windowExpression.isPresent()) {
      keyFormat = getKeyFormat(windowExpression.get());
      step = ExecutionStepFactory.streamWindowedAggregate(
          contextStacker,
          sourceStep,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          windowExpression.get().getKsqlWindowExpression(),
          functionRegistry
      );
    } else {
      keyFormat = this.keyFormat;
      step = ExecutionStepFactory.streamAggregate(
          contextStacker,
          sourceStep,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          functionRegistry
      );
    }

    return new SchemaKTable(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry
    );
  }

  private KeyFormat getKeyFormat(final WindowExpression windowExpression) {
    return KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        windowExpression.getKsqlWindowExpression().getWindowInfo()
    );
  }
}
