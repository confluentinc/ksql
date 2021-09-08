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

package io.confluent.ksql.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.plan.StreamGroupByV1;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSelectKeyV1;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableGroupByV1;
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Builds a string describing a given execution plan. The string describes the plan DAG,
 * along with a name, schema, and processing logger ID for each step. Currently, this
 * description is returned in KSQL's HTTP API in response to EXPLAIN statements.
 */
public class PlanSummary {

  private static final FormatOptions FORMAT_OPTIONS = FormatOptions.of(
      IdentifierUtil::needsQuotes
  );

  @SuppressWarnings("rawtypes")
  private static final Map<Class<? extends ExecutionStep>, String> OP_NAME =
      new ImmutableMap.Builder<Class<? extends ExecutionStep>, String>()
          .put(StreamAggregate.class, "AGGREGATE")
          .put(StreamWindowedAggregate.class, "AGGREGATE")
          .put(StreamFilter.class, "FILTER")
          .put(StreamFlatMap.class, "FLAT_MAP")
          .put(StreamGroupByV1.class, "GROUP_BY")
          .put(StreamGroupBy.class, "GROUP_BY")
          .put(StreamGroupByKey.class, "GROUP_BY")
          .put(StreamSelect.class, "PROJECT")
          .put(StreamSelectKeyV1.class, "REKEY")
          .put(StreamSelectKey.class, "REKEY")
          .put(StreamSink.class, "SINK")
          .put(StreamSource.class, "SOURCE")
          .put(StreamStreamJoin.class, "JOIN")
          .put(StreamTableJoin.class, "JOIN")
          .put(WindowedStreamSource.class, "SOURCE")
          .put(TableAggregate.class, "AGGREGATE")
          .put(TableFilter.class, "FILTER")
          .put(TableGroupByV1.class, "GROUP_BY")
          .put(TableGroupBy.class, "GROUP_BY")
          .put(TableSelect.class, "PROJECT")
          .put(TableSelectKey.class, "REKEY")
          .put(TableSink.class, "SINK")
          .put(TableTableJoin.class, "JOIN")
          .put(ForeignKeyTableTableJoin.class, "JOIN")
          .put(TableSourceV1.class, "SOURCE")
          .put(TableSource.class, "SOURCE")
          .put(TableSuppress.class, "SUPPRESS")
          .put(WindowedTableSource.class, "SOURCE")
          .build();

  private final QueryId queryId;
  private final StepSchemaResolver schemaResolver;

  public PlanSummary(final QueryId queryId, final KsqlConfig config, final MetaStore metaStore) {
    this(queryId, new StepSchemaResolver(config, metaStore));
  }

  @VisibleForTesting
  PlanSummary(final QueryId queryId, final StepSchemaResolver schemaResolver) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.schemaResolver = Objects.requireNonNull(schemaResolver);
  }

  /**
   * Summarize an execution plan.
   * @param step the sink step of the plan.
   * @return A string describing the given plan.
   */
  public String summarize(final ExecutionStep<?> step) {
    return summarize(step, "").summary;
  }

  private StepSummary summarize(final ExecutionStep<?> step, final String indent) {
    final StringBuilder stringBuilder = new StringBuilder();

    final List<StepSummary> sourceSummaries = step.getSources().stream()
        .map(s -> summarize(s, indent + "\t"))
        .collect(Collectors.toList());

    final String opName = OP_NAME.get(step.getClass());
    if (opName == null) {
      throw new UnsupportedOperationException("Unsupported step type: "
          + step.getClass() + ", please add a step type");
    }

    final LogicalSchema schema = getSchema(step, sourceSummaries);

    stringBuilder.append(indent)
        .append(" > [ ")
        .append(opName).append(" ] | Schema: ")
        .append(schema.toString(FORMAT_OPTIONS))
        .append(" | Logger: ")
        .append(QueryLoggerUtil.queryLoggerName(queryId, step.getProperties().getQueryContext()))
        .append("\n");

    for (final StepSummary sourceSummary : sourceSummaries) {
      stringBuilder
          .append("\t")
          .append(indent)
          .append(sourceSummary.summary);
    }
    return new StepSummary(schema, stringBuilder.toString());
  }

  private LogicalSchema getSchema(
      final ExecutionStep<?> step,
      final List<StepSummary> sourceSummaries
  ) {
    switch (sourceSummaries.size()) {
      case 1:
        return schemaResolver.resolve(step, sourceSummaries.get(0).schema);
      case 2:
        return schemaResolver.resolve(
            step,
            sourceSummaries.get(0).schema,
            sourceSummaries.get(1).schema
        );
      case 0:
        return schemaResolver.resolve(step, ((SourceStep<?>) step).getSourceSchema());
      default:
        throw new IllegalStateException();
    }
  }

  private static final class StepSummary {
    private final LogicalSchema schema;
    private final String summary;

    private StepSummary(final LogicalSchema schema, final String summary) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.summary = Objects.requireNonNull(summary, "summary");
    }
  }
}
