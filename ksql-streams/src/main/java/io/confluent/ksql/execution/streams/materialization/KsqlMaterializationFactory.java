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

package io.confluent.ksql.execution.streams.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.Transform;
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Factor class for {@link KsqlMaterialization}.
 */
public final class KsqlMaterializationFactory {

  private static final String FILTER_OP_NAME = "filter";
  private static final String PROJECT_OP_NAME = "project";

  private final ProcessingLogContext processingLogContext;
  private final MaterializationFactory materializationFactory;

  public KsqlMaterializationFactory(
      final ProcessingLogContext processingLogContext
  ) {
    this(
        processingLogContext,
        KsqlMaterialization::new
    );
  }

  @VisibleForTesting
  KsqlMaterializationFactory(
      final ProcessingLogContext processingLogContext,
      final MaterializationFactory materializationFactory
  ) {
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.materializationFactory = requireNonNull(materializationFactory, "materializationFactory");
  }

  public Materialization create(
      final Materialization delegate,
      final MaterializationInfo info,
      final QueryId queryId,
      final QueryContext.Stacker contextStacker
  ) {
    final TransformVisitor transformVisitor = new TransformVisitor(queryId, contextStacker);
    final List<Transform> transforms = info
        .getTransforms()
        .stream()
        .map(xform -> xform.visit(transformVisitor))
        .collect(Collectors.toList());

    return materializationFactory.create(
        delegate,
        info.getSchema(),
        transforms
    );
  }

  interface MaterializationFactory {

    KsqlMaterialization create(
        Materialization inner,
        LogicalSchema schema,
        List<Transform> transforms
    );
  }

  private class TransformVisitor implements MaterializationInfo.TransformVisitor<Transform> {

    private final QueryId queryId;
    private final QueryContext.Stacker stacker;

    TransformVisitor(final QueryId queryId, final QueryContext.Stacker stacker) {
      this.queryId = Objects.requireNonNull(queryId, "queryId");
      this.stacker = Objects.requireNonNull(stacker, "stacker");
    }

    @Override
    public Transform visit(
        final MaterializationInfo.AggregateMapInfo info
    ) {
      final Function<GenericRow, GenericRow> mapper = info
          .getAggregator()
          .getResultMapper()::apply;

      return (k, v) -> Optional.of(mapper.apply(v));
    }

    @Override
    public Transform visit(
        final MaterializationInfo.SqlPredicateInfo info
    ) {
      final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
          QueryLoggerUtil.queryLoggerName(queryId, stacker.push(FILTER_OP_NAME).getQueryContext())
      );

      final Predicate<Object, GenericRow> predicate = info.getPredicate(logger);

      return (k, v) -> predicate.test(k, v) ? Optional.of(v) : Optional.empty();
    }

    @Override
    public Transform visit(
        final MaterializationInfo.ProjectInfo info
    ) {
      final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
          QueryLoggerUtil.queryLoggerName(queryId, stacker.push(PROJECT_OP_NAME).getQueryContext())
      );

      final KsqlValueTransformerWithKey<Object> transformer = info.getSelectTransformer(logger);

      return (k, v) -> Optional.of(transformer.transform(k, v));
    }
  }
}
