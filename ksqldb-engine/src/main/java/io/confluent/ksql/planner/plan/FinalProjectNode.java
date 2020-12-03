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

package io.confluent.ksql.planner.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.Name;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The user supplied projection.
 *
 * <p>Used by all plans except those with GROUP BY, which has its own custom projection handling
 * in {@link AggregateNode}.
 */
public class FinalProjectNode extends ProjectNode implements VerifiableNode {

  private final Projection projection;
  private final Optional<Analysis.Into> into;
  private final LogicalSchema schema;
  private final ImmutableList<SelectExpression> selectExpressions;
  private final Optional<LogicalSchema> pullQueryOutputSchema;
  private final Optional<LogicalSchema> pullQueryIntermediateSchema;
  private final Optional<List<ExpressionMetadata>> compiledSelectExpressions;
  private final RewrittenAnalysis analysis;
  private final boolean isPullQuerySelectStar;
  private final boolean addAdditionalColumnsToIntermediateSchema;

  public FinalProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectItem> selectItems,
      final Optional<Analysis.Into> into,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final RewrittenAnalysis analysis
  ) {
    super(id, source);
    this.projection = Projection.of(selectItems);
    this.into = into;
    this.analysis = Objects.requireNonNull(analysis, "analysis");

    final Pair<LogicalSchema, List<SelectExpression>> result = build(metaStore);
    this.schema = result.left;
    System.out.println("-----> Project output schema=" + schema);
    this.selectExpressions = ImmutableList.copyOf(result.right);
    if (analysis.isPullQuery()) {
      this.isPullQuerySelectStar = isPullQuerySelectStar();
      this.addAdditionalColumnsToIntermediateSchema = shouldAddAdditionalColumnsInSchema();
      this.pullQueryOutputSchema = Optional.of(buildPullQueryOutputSchema(metaStore));
      this.pullQueryIntermediateSchema = Optional.of(buildPullQueryIntermediateSchema());
      this.compiledSelectExpressions = Optional.of(selectExpressions
          .stream()
          .map(selectExpression -> CodeGenRunner.compileExpression(
              selectExpression.getExpression(),
              "Select",
              pullQueryIntermediateSchema.get(),
              ksqlConfig,
              metaStore
          ))
          .collect(ImmutableList.toImmutableList()));
    } else {
      this.isPullQuerySelectStar = false;
      this.addAdditionalColumnsToIntermediateSchema = false;
      this.pullQueryOutputSchema = Optional.empty();
      this.pullQueryIntermediateSchema = Optional.empty();
      this.compiledSelectExpressions = Optional.empty();
      throwOnEmptyValueOrUnknownColumns();
    }

  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public Optional<List<ExpressionMetadata>> getCompiledSelectExpressions() {
    return compiledSelectExpressions;
  }

  @Override
  public void validateKeyPresent(final SourceName sinkName) {
    getSource().validateKeyPresent(sinkName, projection);
  }

  public Optional<LogicalSchema> getPullQueryOutputSchema() {
    return pullQueryOutputSchema;
  }

  public Optional<LogicalSchema> getPullQueryIntermediateSchema() {
    return pullQueryIntermediateSchema;
  }

  public boolean getIsPullQuerySelectStar() {
    return isPullQuerySelectStar;
  }

  public boolean getAddAdditionalColumnsToIntermediateSchema() {
    return addAdditionalColumnsToIntermediateSchema;
  }

  private Optional<LogicalSchema> getTargetSchema(final MetaStore metaStore) {
    return into.filter(i -> !i.isCreate())
        .map(i -> metaStore.getSource(i.getName()))
        .map(target -> target.getSchema());
  }

  private Pair<LogicalSchema, List<SelectExpression>> build(final MetaStore metaStore) {
    final LogicalSchema parentSchema = getSource().getSchema();
    final Optional<LogicalSchema> targetSchema = getTargetSchema(metaStore);

    final List<SelectExpression> selectExpressions = SelectionUtil
        .buildSelectExpressions(getSource(), projection.selectItems(), targetSchema);

    final LogicalSchema schema =
        SelectionUtil.buildProjectionSchema(parentSchema, selectExpressions, metaStore);

    if (into.isPresent()) {
      // Persistent queries have key columns as value columns - final projection can exclude them:
      final Map<ColumnName, Set<ColumnName>> seenKeyColumns = new HashMap<>();
      selectExpressions.removeIf(se -> {
        if (se.getExpression() instanceof UnqualifiedColumnReferenceExp) {
          final ColumnName columnName = ((UnqualifiedColumnReferenceExp) se.getExpression())
              .getColumnName();

          // Window bounds columns are currently removed if not aliased:
          if (SystemColumns.isWindowBound(columnName) && se.getAlias().equals(columnName)) {
            return true;
          }

          if (parentSchema.isKeyColumn(columnName)) {
            seenKeyColumns.computeIfAbsent(columnName, k -> new HashSet<>()).add(se.getAlias());
            return true;
          }
        }
        return false;
      });

      for (final Entry<ColumnName, Set<ColumnName>> seenKey : seenKeyColumns.entrySet()) {
        if (seenKey.getValue().size() > 1) {
          final String keys = GrammaticalJoiner.and().join(
              seenKey.getValue().stream().map(Name::text).sorted());
          throw new KsqlException("The projection contains a key column (" + seenKey.getKey()
              + ") more than once, aliased as: "
              + keys + "."
              + System.lineSeparator()
              + "Each key column must only be in the projection once. "
              + "If you intended to copy the key into the value, then consider using the "
              + AsValue.NAME + " function to indicate which key reference should be copied."
          );
        }
      }
    }

    final LogicalSchema nodeSchema;
    if (into.isPresent()) {
      nodeSchema = schema.withoutPseudoAndKeyColsInValue();
    } else {
      // Transient queries return key columns in the value, so the projection includes them, and
      // the schema needs to include them too:
      final Builder builder = LogicalSchema.builder();

      builder.keyColumns(parentSchema.key());

      schema.columns()
          .forEach(builder::valueColumn);

      nodeSchema = builder.build();
    }

    return Pair.of(nodeSchema, selectExpressions);
  }

  @VisibleForTesting
  protected LogicalSchema buildPullQueryIntermediateSchema() {
    System.out.println("------> parentSchema: " + getSource().getSchema());
    final LogicalSchema parentSchema = getSource().getSchema();
    System.out.println("------> parentSchema.withoutPseudoAndKeyColsInValue: "
                           + parentSchema.withoutPseudoAndKeyColsInValue());

    if (!addAdditionalColumnsToIntermediateSchema) {
      System.out.println("-----> intermediate schema no additional= " + parentSchema);
      return parentSchema;
    } else {
      // SelectValueMapper requires the rowTime & key fields in the value schema :(
      final boolean isWindowed = analysis
          .getFrom()
          .getDataSource()
          .getKsqlTopic()
          .getKeyFormat().isWindowed();

      System.out.println("-----> intermediate schema= " + parentSchema
          .withPseudoAndKeyColsInValue(isWindowed));
      return parentSchema
          .withPseudoAndKeyColsInValue(isWindowed);
    }
  }

  @VisibleForTesting
  protected LogicalSchema buildPullQueryOutputSchema(final MetaStore metaStore) {
    final LogicalSchema outputSchema;
    final LogicalSchema parentSchema = getSource().getSchema();
    final boolean isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();

    if (isPullQuerySelectStar()) {
      outputSchema = buildPullQuerySelectStarSchema(
          parentSchema.withoutPseudoAndKeyColsInValue(), isWindowed);
      System.out.println("-----> pull query select * output schema= " + outputSchema);
    } else {
      final List<SelectExpression> projects = projection.selectItems().stream()
          .map(SingleColumn.class::cast)
          .map(si -> SelectExpression
              .of(si.getAlias().orElseThrow(IllegalStateException::new), si.getExpression()))
          .collect(Collectors.toList());

      outputSchema = selectOutputSchema(metaStore, projects, isWindowed);
      System.out.println("-----> pull query output schema= " + outputSchema);
    }
    return outputSchema;
  }

  private boolean shouldAddAdditionalColumnsInSchema() {

    final boolean hasSystemColumns = projection.selectItems().stream().anyMatch(se -> {
      if (se instanceof SingleColumn
          && ((SingleColumn)se).getExpression() instanceof  UnqualifiedColumnReferenceExp) {
        final SingleColumn singleColumn = ((SingleColumn)se);
        final ColumnName columnName = ((UnqualifiedColumnReferenceExp) singleColumn.getExpression())
            .getColumnName();
        return SystemColumns.isSystemColumn(columnName);
      }
      return false;
    });

    final boolean hasKeyColumns = projection.selectItems().stream().anyMatch(se -> {
      if (se instanceof SingleColumn
          && ((SingleColumn)se).getExpression() instanceof  UnqualifiedColumnReferenceExp) {
        final SingleColumn singleColumn = ((SingleColumn)se);
        final ColumnName columnName = ((UnqualifiedColumnReferenceExp) singleColumn.getExpression())
            .getColumnName();
        return getSource().getSchema().isKeyColumn(columnName);
      }
      return false;
    });

    return hasSystemColumns || hasKeyColumns;
  }

  private boolean isPullQuerySelectStar() {
    final boolean someStars = projection.selectItems().stream()
        .anyMatch(s -> s instanceof AllColumns);

    if (someStars && projection.selectItems().size() != 1) {
      throw new KsqlException("Pull queries only support wildcards in the projects "
                                  + "if they are the only expression");
    }

    return someStars;
  }

  private LogicalSchema buildPullQuerySelectStarSchema(
      final LogicalSchema schema,
      final boolean windowed
  ) {
    final Builder builder = LogicalSchema.builder()
        .keyColumns(schema.key());

    if (windowed) {
      builder.keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT);
      builder.keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT);
    }

    return builder
        .valueColumns(schema.value())
        .build();
  }

  private LogicalSchema selectOutputSchema(
      final MetaStore metaStore,
      final List<SelectExpression> selectExpressions,
      final boolean isWindowed
  ) {
    final Builder schemaBuilder = LogicalSchema.builder();
    final LogicalSchema parentSchema = getSource().getSchema();

    // Copy meta & key columns into the value schema as SelectValueMapper expects it:
    final LogicalSchema schema = parentSchema
        .withPseudoAndKeyColsInValue(isWindowed);

    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, metaStore);

    for (final SelectExpression select : selectExpressions) {
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());

      if (parentSchema.isKeyColumn(select.getAlias())
          || select.getAlias().equals(SystemColumns.WINDOWSTART_NAME)
          || select.getAlias().equals(SystemColumns.WINDOWEND_NAME)
      ) {
        schemaBuilder.keyColumn(select.getAlias(), type);
      } else {
        schemaBuilder.valueColumn(select.getAlias(), type);
      }
    }
    return schemaBuilder.build();
  }

  private void throwOnEmptyValueOrUnknownColumns() {
    final LogicalSchema schema = getSchema();

    if (schema.value().isEmpty()) {
      throw new KsqlException("The projection contains no value columns.");
    }

    validateProjection();
  }

  /**
   * Called to validate that columns referenced in the projection are valid.
   *
   * <p>This is necessary as some joins can create synthetic key columns that do not come
   * from any data source.  This means the normal column validation done during analysis can not
   * fail on unknown column with generated column names.
   *
   * <p>Once the logical model has been built the synthetic key names are known and generated
   * column names can be validated.
   */
  private void validateProjection() {
    // Validate any column in the projection that might be a synthetic
    // Only really need to include any that might be, but we include all:
    final RequiredColumns requiredColumns = RequiredColumns.builder()
        .addAll(projection.singleExpressions())
        .build();

    final Set<ColumnReferenceExp> unknown = getSource().validateColumns(requiredColumns);
    if (!unknown.isEmpty()) {
      final String errors = unknown.stream()
          .map(columnRef -> NodeLocation.asPrefix(columnRef.getLocation())
              + "Column '" + columnRef + "' cannot be resolved."
          ).collect(Collectors.joining(System.lineSeparator()));

      throw new KsqlException(errors);
    }
  }
}
