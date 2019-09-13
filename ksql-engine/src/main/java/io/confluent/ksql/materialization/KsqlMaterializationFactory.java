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

package io.confluent.ksql.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.SelectValueMapperFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SqlPredicate;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Factor class for {@link KsqlMaterialization}.
 */
public final class KsqlMaterializationFactory {

  private static final String FILTER_OP_NAME = "filter";
  private static final String PROJECT_OP_NAME = "project";

  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;
  private final ProcessingLogContext processingLogContext;
  private final SqlPredicateFactory sqlPredicateFactory;
  private final ValueMapperFactory valueMapperFactory;
  private final MaterializationFactory materializationFactory;

  public KsqlMaterializationFactory(
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogContext processingLogContext
  ) {
    this(
        ksqlConfig,
        functionRegistry,
        processingLogContext,
        SqlPredicate::new,
        defaultValueMapperFactory(),
        KsqlMaterialization::new
    );
  }

  @VisibleForTesting
  KsqlMaterializationFactory(
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogContext processingLogContext,
      final SqlPredicateFactory sqlPredicateFactory,
      final ValueMapperFactory valueMapperFactory,
      final MaterializationFactory materializationFactory
  ) {
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.sqlPredicateFactory = requireNonNull(sqlPredicateFactory, "sqlPredicateFactory");
    this.valueMapperFactory = requireNonNull(valueMapperFactory, "valueMapperFactory");
    this.materializationFactory = requireNonNull(materializationFactory, "materializationFactory");
  }

  public Materialization create(
      final Materialization delegate,
      final MaterializationInfo info,
      final QueryContext.Stacker contextStacker
  ) {
    final Predicate<Struct, GenericRow> havingPredicate =
        bakeHavingExpression(info, contextStacker);

    final Function<GenericRow, GenericRow> valueMapper =
        bakeStoreSelects(info, contextStacker);

    return materializationFactory.create(
        delegate,
        havingPredicate,
        valueMapper,
        info.tableSchema()
    );
  }

  private Predicate<Struct, GenericRow> bakeHavingExpression(
      final MaterializationInfo info,
      final QueryContext.Stacker contextStacker
  ) {
    if (!info.havingExpression().isPresent()) {
      return (k, v) -> true;
    }

    final Expression having = info.havingExpression().get();

    final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
        QueryLoggerUtil.queryLoggerName(contextStacker.push(FILTER_OP_NAME).getQueryContext())
    );

    final SqlPredicate predicate = sqlPredicateFactory.create(
        having,
        info.aggregationSchema(),
        ksqlConfig,
        functionRegistry,
        logger
    );

    return predicate.getPredicate();
  }

  private Function<GenericRow, GenericRow> bakeStoreSelects(
      final MaterializationInfo info,
      final Stacker contextStacker
  ) {
    final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
        QueryLoggerUtil.queryLoggerName(contextStacker.push(PROJECT_OP_NAME).getQueryContext())
    );

    return valueMapperFactory.create(
        info.tableSelects(),
        info.aggregationSchema(),
        ksqlConfig,
        functionRegistry,
        logger
    );
  }

  private static ValueMapperFactory defaultValueMapperFactory() {
    return (selectExpressions, sourceSchema, ksqlConfig, functionRegistry, processingLogger) ->
        SelectValueMapperFactory.create(
            selectExpressions,
            sourceSchema,
            ksqlConfig,
            functionRegistry,
            processingLogger
        )::apply;
  }

  interface SqlPredicateFactory {

    SqlPredicate create(
        Expression filterExpression,
        LogicalSchema schema,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger processingLogger
    );
  }

  interface ValueMapperFactory {

    Function<GenericRow, GenericRow> create(
        List<SelectExpression> selectExpressions,
        LogicalSchema sourceSchema,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        ProcessingLogger processingLogger
    );
  }

  interface MaterializationFactory {

    KsqlMaterialization create(
        Materialization inner,
        Predicate<Struct, GenericRow> havingPredicate,
        Function<GenericRow, GenericRow> storeToTableTransform,
        LogicalSchema schema
    );
  }
}
