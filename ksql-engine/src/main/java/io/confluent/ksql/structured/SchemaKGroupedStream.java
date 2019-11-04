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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;

public class SchemaKGroupedStream {

  final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep;
  final KeyFormat keyFormat;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;

  SchemaKGroupedStream(
      final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep,
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

  public ExecutionStep<KGroupedStream<Struct, GenericRow>> getSourceStep() {
    return sourceStep;
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<?> aggregate(
      final LogicalSchema aggregateSchema,
      final LogicalSchema outputSchema,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker,
      final KsqlQueryBuilder queryBuilder
  ) {
    throwOnValueFieldCountMismatch(outputSchema, nonFuncColumnCount, aggregations);

    final ExecutionStep<? extends KTableHolder<?>> step;
    final KeyFormat keyFormat;

    if (windowExpression.isPresent()) {
      keyFormat = getKeyFormat(windowExpression.get());
      step = ExecutionStepFactory.streamWindowedAggregate(
          contextStacker,
          sourceStep,
          outputSchema,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          aggregateSchema,
          windowExpression.get().getKsqlWindowExpression()
      );
    } else {
      keyFormat = this.keyFormat;
      step = ExecutionStepFactory.streamAggregate(
          contextStacker,
          sourceStep,
          outputSchema,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          aggregateSchema
      );
    }

    return new SchemaKTable(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry
    );
  }

  private KeyFormat getKeyFormat(final WindowExpression windowExpression) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)) {
      return KeyFormat.windowed(
          FormatInfo.of(Format.KAFKA),
          WindowInfo.of(
              WindowType.TUMBLING,
              Optional.of(Duration.ofMillis(Long.MAX_VALUE))
          )
      );
    }
    return KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        windowExpression.getKsqlWindowExpression().getWindowInfo()
    );
  }

  static void throwOnValueFieldCountMismatch(
      final LogicalSchema aggregateSchema,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregateFunctions
  ) {
    final int totalColumnCount = aggregateFunctions.size() + nonFuncColumnCount;

    final int valueColumnCount = aggregateSchema.value().size();
    if (valueColumnCount != totalColumnCount) {
      throw new IllegalArgumentException(
          "Aggregate schema value field count does not match expected."
          + " expected: " + totalColumnCount
          + ", actual: " + valueColumnCount
          + ", schema: " + aggregateSchema
      );
    }
  }
}
