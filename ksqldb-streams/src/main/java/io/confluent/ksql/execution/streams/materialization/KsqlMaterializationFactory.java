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
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.MapperInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.PredicateInfo;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.Transform;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Factor class for {@link KsqlMaterialization}.
 */
public final class KsqlMaterializationFactory {

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
      final StreamsMaterialization delegate,
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
        StreamsMaterialization inner,
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
        final MapperInfo info
    ) {
      final KsqlTransformer<Object, GenericRow> resultMapper = info
          .getMapper(this::getLogger);

      return (k, v, ctx) -> Optional.of(resultMapper.transform(k, v));
    }

    @Override
    public Transform visit(
        final PredicateInfo info
    ) {
      final KsqlTransformer<Object, Optional<GenericRow>> predicate = info
          .getPredicate(this::getLogger);

      return (readOnlyKey, value, ctx) -> predicate.transform(readOnlyKey, value);
    }

    private ProcessingLogger getLogger(final QueryContext queryContext) {
      QueryContext.Stacker stacker = this.stacker;
      for (final String ctx : queryContext.getContext()) {
        stacker = stacker.push(ctx);
      }
      return processingLogContext.getLoggerFactory().getLogger(
          QueryLoggerUtil.queryLoggerName(QueryType.PULL_QUERY, stacker.getQueryContext())
      );
    }
  }
}
