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

package io.confluent.ksql.analyzer;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class Analyzer {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final String KAFKA_VALUE_FORMAT_LIMITATION_DETAILS = ""
      + "The KAFKA format is primarily intended for use as a key format. "
      + "It can be used as a value format, but can not be used in any operation that "
      + "requires a repartition or changelog topic." + System.lineSeparator()
      + "Removing this limitation requires enhancements to the core of KSQL. "
      + "This will come in a future release. Until then, avoid using the KAFKA format for values."
      + System.lineSeparator() + "If you have an existing topic with "
      + "KAFKA formatted values you can duplicate the data and serialize using Avro or JSON with a "
      + "statement such as: "
      + System.lineSeparator()
      + System.lineSeparator()
      + "'CREATE STREAM <new-stream-name> WITH(VALUE_FORMAT='Avro') AS "
      + "SELECT * FROM <existing-kafka-formated-stream-name>;'"
      + System.lineSeparator()
      + "For more info see https://github.com/confluentinc/ksql/issues/3060";

  private final MetaStore metaStore;
  private final String topicPrefix;
  private final SerdeOptionsSupplier serdeOptionsSupplier;
  private final Set<SerdeOption> defaultSerdeOptions;

  /**
   * @param metaStore the metastore to use.
   * @param topicPrefix the prefix to use for topic names where an explicit name is not specified.
   * @param defaultSerdeOptions the default serde options.
   */
  Analyzer(
      final MetaStore metaStore,
      final String topicPrefix,
      final Set<SerdeOption> defaultSerdeOptions
  ) {
    this(
        metaStore,
        topicPrefix,
        defaultSerdeOptions,
        SerdeOptions::buildForCreateAsStatement);
  }

  @VisibleForTesting
  Analyzer(
      final MetaStore metaStore,
      final String topicPrefix,
      final Set<SerdeOption> defaultSerdeOptions,
      final SerdeOptionsSupplier serdeOptionsSupplier
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix");
    this.defaultSerdeOptions = ImmutableSet
        .copyOf(requireNonNull(defaultSerdeOptions, "defaultSerdeOptions"));
    this.serdeOptionsSupplier = requireNonNull(serdeOptionsSupplier, "serdeOptionsSupplier");
  }

  /**
   * Analyze the query.
   *
   * @param query the query to analyze.
   * @param sink the sink the query will output to.
   * @return the analysis.
   */
  Analysis analyze(
      final Query query,
      final Optional<Sink> sink
  ) {
    final Visitor visitor = new Visitor(query);
    visitor.process(query, null);

    sink.ifPresent(visitor::analyzeNonStdOutSink);

    visitor.validate();

    return visitor.analysis;
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private final class Visitor extends DefaultTraversalVisitor<AstNode, Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final Analysis analysis;
    private final boolean pullQuery;
    private boolean isJoin = false;
    private boolean isGroupBy = false;

    Visitor(final Query query) {
      this.pullQuery = query.isPullQuery();
      this.analysis = new Analysis(query.getResultMaterialization());
    }

    private void analyzeNonStdOutSink(final Sink sink) {
      analysis.setProperties(sink.getProperties());
      sink.getPartitionBy()
          .map(name -> ColumnRef.withoutSource(name.name()))
          .ifPresent(analysis::setPartitionBy);

      setSerdeOptions(sink);

      if (!sink.shouldCreateSink()) {
        final DataSource<?> existing = metaStore.getSource(sink.getName());
        if (existing == null) {
          throw new KsqlException("Unknown source: "
              + sink.getName().toString(FormatOptions.noEscape()));
        }

        analysis.setInto(Into.of(
            sink.getName(),
            false,
            existing.getKsqlTopic()
        ));
        return;
      }

      final String topicName = sink.getProperties().getKafkaTopic()
          .orElseGet(() -> topicPrefix + sink.getName().name());

      final KeyFormat keyFormat = buildKeyFormat();

      final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(
          getValueFormat(sink),
          sink.getProperties().getValueAvroSchemaName(),
          getValueDelimiter(sink)
      ));

      final KsqlTopic intoKsqlTopic = new KsqlTopic(
          topicName,
          keyFormat,
          valueFormat,
          true
      );

      analysis.setInto(Into.of(
          sink.getName(),
          true,
          intoKsqlTopic
      ));
    }

    private KeyFormat buildKeyFormat() {
      final Optional<KsqlWindowExpression> ksqlWindow = analysis.getWindowExpression()
          .map(WindowExpression::getKsqlWindowExpression);

      return ksqlWindow
          .map(w -> KeyFormat.windowed(FormatInfo.of(Format.KAFKA), w.getWindowInfo()))
          .orElseGet(() -> analysis
              .getFromDataSources()
              .get(0)
              .getDataSource()
              .getKsqlTopic()
              .getKeyFormat());
    }

    private void setSerdeOptions(final Sink sink) {
      final List<ColumnName> columnNames = getNoneMetaOrKeySelectAliases();

      final Format valueFormat = getValueFormat(sink);

      final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
          columnNames,
          valueFormat,
          sink.getProperties().getWrapSingleValues(),
          defaultSerdeOptions
      );

      analysis.setSerdeOptions(serdeOptions);
    }

    /**
     * Get the list of select expressions that are <i>not</i> for meta and key columns in the source
     * schema.
     *
     * <p>Currently, the select expressions can include metadata and key columns, which are later
     * removed by {@link io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode}. When building
     * the {@link SerdeOptions} its important the final schema is used, i.e. the one with these
     * meta and key columns removed.
     *
     * <p>This is weird functionality, but maintained for backwards compatibility for now.
     *
     * @return the list of column names in the sink that are not meta or key columns.
     */
    private List<ColumnName> getNoneMetaOrKeySelectAliases() {
      final SourceSchemas sourceSchemas = analysis.getFromSourceSchemas();
      final List<SelectExpression> selects = analysis.getSelectExpressions();

      final List<ColumnName> columnNames = analysis.getSelectExpressions().stream()
          .map(SelectExpression::getAlias)
          .collect(Collectors.toList());

      for (int idx = selects.size() - 1; idx >= 0; --idx) {
        final SelectExpression select = selects.get(idx);
        final Expression expression = select.getExpression();

        if (!(expression instanceof ColumnReferenceExp)) {
          continue;
        }

        if (!sourceSchemas.matchesNonValueField(((ColumnReferenceExp) expression).getReference())) {
          continue;
        }

        final ColumnName columnName = select.getAlias();
        if (columnName.equals(SchemaUtil.ROWTIME_NAME)
            || columnName.equals(SchemaUtil.ROWKEY_NAME)) {
          columnNames.remove(idx);
        }
      }
      return columnNames;
    }

    private Format getValueFormat(final Sink sink) {
      return sink.getProperties().getValueFormat()
          .orElseGet(() -> analysis
              .getFromDataSources()
              .get(0)
              .getDataSource()
              .getKsqlTopic()
              .getValueFormat()
              .getFormat());
    }

    private Optional<Delimiter> getValueDelimiter(final Sink sink) {
      if (sink.getProperties().getValueDelimiter().isPresent()) {
        return sink.getProperties().getValueDelimiter();
      }
      return analysis
          .getFromDataSources()
          .get(0)
          .getDataSource()
          .getKsqlTopic()
          .getValueFormat()
          .getFormatInfo()
          .getDelimiter();
    }


    @Override
    protected AstNode visitQuery(
        final Query node,
        final Void context
    ) {
      process(node.getFrom(), context);

      process(node.getSelect(), context);

      node.getWhere().ifPresent(this::analyzeWhere);
      node.getGroupBy().ifPresent(this::analyzeGroupBy);
      node.getWindow().ifPresent(this::analyzeWindowExpression);
      node.getHaving().ifPresent(this::analyzeHaving);
      node.getLimit().ifPresent(analysis::setLimitClause);

      throwOnUnknownColumnReference();

      return null;
    }

    private void throwOnUnknownColumnReference() {

      final ExpressionAnalyzer expressionAnalyzer =
          new ExpressionAnalyzer(analysis.getFromSourceSchemas());

      for (final SelectExpression selectExpression : analysis.getSelectExpressions()) {
        expressionAnalyzer.analyzeExpression(selectExpression.getExpression(), false);
      }

      analysis.getWhereExpression().ifPresent(where -> {
        final boolean allowWindowMetaFields = pullQuery
            && analysis.getFromDataSources().get(0)
            .getDataSource()
            .getKsqlTopic()
            .getKeyFormat()
            .isWindowed();

        expressionAnalyzer.analyzeExpression(where, allowWindowMetaFields);
      });

      for (final Expression expression : analysis.getGroupByExpressions()) {
        expressionAnalyzer.analyzeExpression(expression, false);
      }

      analysis.getHavingExpression().ifPresent(having ->
          expressionAnalyzer.analyzeExpression(having, false)
      );
    }

    @Override
    protected AstNode visitJoin(final Join node, final Void context) {
      isJoin = true;

      process(node.getLeft(), context);
      process(node.getRight(), context);

      final JoinNode.JoinType joinType = getJoinType(node);

      final AliasedDataSource left = analysis.getFromDataSources().get(0);
      final AliasedDataSource right = analysis.getFromDataSources().get(1);

      final JoinOn joinOn = (JoinOn) node.getCriteria();
      final ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn
          .getExpression();

      if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
        throw new KsqlException("Only equality join criteria is supported.");
      }

      final ColumnRef leftJoinField = getJoinFieldName(
          comparisonExpression,
          left.getAlias(),
          left.getDataSource().getSchema()
      );

      final ColumnRef rightJoinField = getJoinFieldName(
          comparisonExpression,
          right.getAlias(),
          right.getDataSource().getSchema()
      );

      analysis.setJoin(new JoinInfo(
          leftJoinField,
          rightJoinField,
          joinType,
          node.getWithinExpression()
      ));

      return null;
    }

    private JoinNode.JoinType getJoinType(final Join node) {
      final JoinNode.JoinType joinType;
      switch (node.getType()) {
        case INNER:
          joinType = JoinNode.JoinType.INNER;
          break;
        case LEFT:
          joinType = JoinNode.JoinType.LEFT;
          break;
        case OUTER:
          joinType = JoinNode.JoinType.OUTER;
          break;
        default:
          throw new KsqlException("Join type is not supported: " + node.getType().name());
      }
      return joinType;
    }

    private ColumnReferenceExp checkExpressionType(
        final ComparisonExpression comparisonExpression,
        final Expression subExpression) {

      if (!(subExpression instanceof ColumnReferenceExp)) {
        throw new KsqlException(
            String.format(
                "%s : Invalid comparison expression '%s' in join '%s'. Joins must only contain a "
                    + "field comparison.",
                comparisonExpression.getLocation().map(Objects::toString).orElse(""),
                subExpression,
                comparisonExpression
            )
        );
      }
      return (ColumnReferenceExp) subExpression;
    }

    private ColumnRef getJoinFieldName(
        final ComparisonExpression comparisonExpression,
        final SourceName sourceAlias,
        final LogicalSchema sourceSchema
    ) {
      final ColumnReferenceExp left =
          checkExpressionType(comparisonExpression, comparisonExpression.getLeft());

      Optional<ColumnRef> joinFieldName = getJoinFieldNameFromExpr(left, sourceAlias);

      if (!joinFieldName.isPresent()) {
        final ColumnReferenceExp right =
            checkExpressionType(comparisonExpression, comparisonExpression.getRight());

        joinFieldName = getJoinFieldNameFromExpr(right, sourceAlias);

        if (!joinFieldName.isPresent()) {
          // Should never happen as only QualifiedNameReference are allowed
          throw new IllegalStateException("Cannot find join field name");
        }
      }

      final ColumnRef fieldName = joinFieldName.get();

      final Optional<ColumnRef> joinField =
          getJoinFieldNameFromSource(fieldName.withoutSource(), sourceAlias, sourceSchema);

      return joinField
          .orElseThrow(() -> new KsqlException(
              String.format(
                  "%s : Invalid join criteria %s. Column %s.%s does not exist.",
                  comparisonExpression.getLocation().map(Objects::toString).orElse(""),
                  comparisonExpression,
                  sourceAlias.name(),
                  fieldName.name().toString(FormatOptions.noEscape())
              )
          ));
    }

    private Optional<ColumnRef> getJoinFieldNameFromExpr(
        final ColumnReferenceExp nameRef,
        final SourceName sourceAlias
    ) {
      if (nameRef.getReference().source().isPresent()
          && !nameRef.getReference().source().get().equals(sourceAlias)) {
        return Optional.empty();
      }

      final ColumnRef fieldName = nameRef.getReference();
      return Optional.of(fieldName);
    }

    private Optional<ColumnRef> getJoinFieldNameFromSource(
        final ColumnRef fieldName,
        final SourceName sourceAlias,
        final LogicalSchema sourceSchema
    ) {
      return sourceSchema.findColumn(fieldName)
          .map(Column::ref)
          .map(ref -> ref.withSource(sourceAlias));
    }

    @Override
    protected AstNode visitAliasedRelation(final AliasedRelation node, final Void context) {
      final SourceName structuredDataSourceName = ((Table) node.getRelation()).getName();

      final DataSource<?> source = metaStore.getSource(structuredDataSourceName);
      if (source == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      analysis.addDataSource(node.getAlias(), source);
      return node;
    }

    @Override
    protected AstNode visitSelect(final Select node, final Void context) {
      for (final SelectItem selectItem : node.getSelectItems()) {
        if (selectItem instanceof AllColumns) {
          visitSelectStar((AllColumns) selectItem);
        } else if (selectItem instanceof SingleColumn) {
          final SingleColumn column = (SingleColumn) selectItem;
          addSelectItem(column.getExpression(), column.getAlias().get());
          visitTableFunctions(column.getExpression());
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + selectItem.getClass().getName());
        }
      }
      return null;
    }

    @Override
    protected AstNode visitGroupBy(final GroupBy node, final Void context) {
      return null;
    }

    private void analyzeWhere(final Expression node) {
      analysis.setWhereExpression(node);
    }

    private void analyzeGroupBy(final GroupBy groupBy) {
      isGroupBy = true;

      for (final GroupingElement groupingElement : groupBy.getGroupingElements()) {
        final Set<Expression> groupingSet = groupingElement.enumerateGroupingSets().get(0);
        analysis.addGroupByExpressions(groupingSet);
      }
    }

    private void analyzeWindowExpression(final WindowExpression windowExpression) {
      analysis.setWindowExpression(windowExpression);
    }

    private void analyzeHaving(final Expression node) {
      analysis.setHavingExpression(node);
    }

    private void visitSelectStar(final AllColumns allColumns) {

      final Optional<NodeLocation> location = allColumns.getLocation();

      final Optional<SourceName> prefix = allColumns.getSource();

      for (final AliasedDataSource source : analysis.getFromDataSources()) {

        if (prefix.isPresent() && !prefix.get().equals(source.getAlias())) {
          continue;
        }

        final String aliasPrefix = analysis.isJoin()
            ? source.getAlias().name() + "_"
            : "";

        final LogicalSchema schema = source.getDataSource().getSchema();
        for (final Column column : schema.columns()) {

          if (pullQuery && schema.isMetaColumn(column.name())) {
            continue;
          }

          final ColumnReferenceExp selectItem = new ColumnReferenceExp(location,
              ColumnRef.of(source.getAlias(), column.name()));

          final String alias = aliasPrefix + column.name().name();

          addSelectItem(selectItem, ColumnName.of(alias));
        }
      }
    }

    public void validate() {
      final String kafkaSources = analysis.getFromDataSources().stream()
          .filter(s -> s.getDataSource().getKsqlTopic().getValueFormat().getFormat()
              == Format.KAFKA)
          .map(AliasedDataSource::getAlias)
          .map(SourceName::name)
          .collect(Collectors.joining(", "));

      if (kafkaSources.isEmpty()) {
        return;
      }

      if (isJoin) {
        throw new KsqlException("Source(s) " + kafkaSources + " are using the 'KAFKA' value format."
            + " This format does not yet support JOIN."
            + System.lineSeparator() + KAFKA_VALUE_FORMAT_LIMITATION_DETAILS);
      }

      if (isGroupBy) {
        throw new KsqlException("Source(s) " + kafkaSources + " are using the 'KAFKA' value format."
            + " This format does not yet support GROUP BY."
            + System.lineSeparator() + KAFKA_VALUE_FORMAT_LIMITATION_DETAILS);
      }
    }

    private void addSelectItem(final Expression exp, final ColumnName columnName) {
      final Set<ColumnRef> columnRefs = new HashSet<>();
      final TraversalExpressionVisitor<Void> visitor = new TraversalExpressionVisitor<Void>() {
        @Override
        public Void visitColumnReference(
            final ColumnReferenceExp node,
            final Void context
        ) {
          columnRefs.add(node.getReference());
          return null;
        }
      };

      visitor.process(exp, null);

      analysis.addSelectItem(exp, columnName);
      analysis.addSelectColumnRefs(columnRefs);
    }

    private void visitTableFunctions(final Expression expression) {
      final TableFunctionVisitor visitor = new TableFunctionVisitor();
      visitor.process(expression, null);
    }

    private final class TableFunctionVisitor extends TraversalExpressionVisitor<Void> {

      private Optional<String> tableFunctionName = Optional.empty();

      @Override
      public Void visitFunctionCall(final FunctionCall functionCall, final Void context) {
        final String functionName = functionCall.getName().name();
        final boolean isTableFunction = metaStore.isTableFunction(functionName);

        if (isTableFunction) {
          if (tableFunctionName.isPresent()) {
            throw new KsqlException("Table functions cannot be nested: "
                + tableFunctionName.get() + "(" + functionName + "())");
          }

          tableFunctionName = Optional.of(functionName);

          analysis.addTableFunction(functionCall);
        }

        super.visitFunctionCall(functionCall, context);

        if (isTableFunction) {
          tableFunctionName = Optional.empty();
        }

        return null;
      }
    }
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        List<ColumnName> valueColumnNames,
        Format valueFormat,
        Optional<Boolean> wrapSingleValues,
        Set<SerdeOption> singleFieldDefaults
    );
  }
}
