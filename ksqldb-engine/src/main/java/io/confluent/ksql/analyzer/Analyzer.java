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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.UnknownSourceException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  /**
   * @param metaStore the metastore to use.
   * @param topicPrefix the prefix to use for topic names where an explicit name is not specified.
   */
  Analyzer(
      final MetaStore metaStore,
      final String topicPrefix
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix");
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
    final Visitor visitor = new Visitor(query, sink.isPresent());

    visitor.process(query, null);

    sink.ifPresent(visitor::analyzeNonStdOutSink);

    visitor.validate();

    return visitor.analysis;
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private final class Visitor extends DefaultTraversalVisitor<AstNode, Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final Analysis analysis;
    private final boolean persistent;
    private boolean isJoin = false;
    private boolean isGroupBy = false;

    Visitor(final Query query, final boolean persistent) {
      this.analysis = new Analysis(query.getRefinement());
      this.persistent = persistent;
    }

    private void analyzeNonStdOutSink(final Sink sink) {
      analysis.setProperties(sink.getProperties());

      if (!sink.shouldCreateSink()) {
        final DataSource existing = metaStore.getSource(sink.getName());
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
          .orElseGet(() -> topicPrefix + sink.getName().text());

      final KeyFormat keyFormat = buildKeyFormat();
      final Format format = getValueFormat(sink);

      final Map<String, String> sourceProperties = new HashMap<>();
      if (format.name().equals(getSourceInfo().getFormat())) {
        getSourceInfo().getProperties().forEach((k, v) -> {
          if (format.getInheritableProperties().contains(k)) {
            sourceProperties.put(k, v);
          }
        });
      }

      // overwrite any inheritable properties if they were explicitly
      // specified in the statement
      sourceProperties.putAll(sink.getProperties().getFormatProperties());

      final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(
          format.name(),
          sourceProperties
      ));

      final KsqlTopic intoKsqlTopic = new KsqlTopic(
          topicName,
          keyFormat,
          valueFormat
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
          .map(w -> KeyFormat.windowed(
              FormatInfo.of(FormatFactory.KAFKA.name()), w.getWindowInfo()))
          .orElseGet(() -> analysis
              .getFrom()
              .getDataSource()
              .getKsqlTopic()
              .getKeyFormat());
    }

    private Format getValueFormat(final Sink sink) {
      return sink.getProperties().getValueFormat()
          .orElseGet(() -> FormatFactory.of(getSourceInfo()));
    }

    private FormatInfo getSourceInfo() {
      return analysis
          .getFrom()
          .getDataSource()
          .getKsqlTopic()
          .getValueFormat()
          .getFormatInfo();
    }


    @Override
    protected AstNode visitQuery(
        final Query node,
        final Void context
    ) {
      process(node.getFrom(), context);

      node.getWhere().ifPresent(this::analyzeWhere);
      node.getGroupBy().ifPresent(this::analyzeGroupBy);
      node.getPartitionBy().ifPresent(this::analyzePartitionBy);
      node.getWindow().ifPresent(this::analyzeWindowExpression);
      node.getHaving().ifPresent(this::analyzeHaving);
      node.getLimit().ifPresent(analysis::setLimitClause);

      process(node.getSelect(), context);

      throwOnUnknownColumnReference(
          !node.isPullQuery() && !node.getGroupBy().isPresent()
      );

      return null;
    }

    private void throwOnUnknownColumnReference(final boolean possibleSyntheticColumns) {

      final ColumnReferenceValidator columnValidator = new ColumnReferenceValidator(
          analysis.getFromSourceSchemas(true),
          possibleSyntheticColumns
      );

      analysis.getWhereExpression()
          .ifPresent(expression -> columnValidator.analyzeExpression(expression, "WHERE"));

      analysis.getGroupBy()
          .map(GroupBy::getGroupingExpressions)
          .orElseGet(ImmutableList::of)
          .forEach(expression -> columnValidator.analyzeExpression(expression, "GROUP BY"));

      analysis.getPartitionBy()
          .map(PartitionBy::getExpression)
          .ifPresent(expression -> columnValidator.analyzeExpression(expression, "PARTITION BY"));

      analysis.getHavingExpression()
          .ifPresent(expression -> columnValidator.analyzeExpression(expression, "HAVING"));

      analysis.getSelectItems().stream()
          .filter(si -> si instanceof SingleColumn)
          .map(SingleColumn.class::cast)
          .map(SingleColumn::getExpression)
          .forEach(expression -> columnValidator.analyzeExpression(expression, "SELECT"));
    }

    @Override
    protected AstNode visitJoin(final Join node, final Void context) {
      isJoin = true;

      process(node.getLeft(), context);
      node.getRights().forEach(right -> process(right, context));
      return null;
    }

    @Override
    protected AstNode visitJoinedSource(final JoinedSource node, final Void context) {
      process(node.getRelation(), context);

      final AliasedDataSource source = analysis
              .getSourceByAlias(((AliasedRelation) node.getRelation()).getAlias())
              .orElseThrow(() -> new IllegalStateException(
                      "Expected to register source in above process call"));

      final JoinNode.JoinType joinType = getJoinType(node);

      final JoinOn joinOn = (JoinOn) node.getCriteria();
      final ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn
          .getExpression();

      if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
        throw new KsqlException("Only equality join criteria is supported.");
      }

      final ColumnReferenceValidator columnValidator =
          new ColumnReferenceValidator(analysis.getFromSourceSchemas(false), false);

      final Set<SourceName> srcsUsedInLeft = columnValidator
          .analyzeExpression(comparisonExpression.getLeft(), "JOIN ON");

      final Set<SourceName> srcsUsedInRight = columnValidator
          .analyzeExpression(comparisonExpression.getRight(), "JOIN ON");

      final SourceName leftSourceName = getOnlySourceForJoin(
          comparisonExpression.getLeft(), comparisonExpression, srcsUsedInLeft);
      final SourceName rightSourceName = getOnlySourceForJoin(
          comparisonExpression.getRight(), comparisonExpression, srcsUsedInRight);

      final AliasedDataSource left = analysis.getSourceByAlias(leftSourceName)
          .orElseThrow(() -> new KsqlException("Cannot join on unknown source: " + leftSourceName));

      final AliasedDataSource right = analysis.getSourceByAlias(rightSourceName)
          .orElseThrow(() -> new KsqlException("Cannot join on unknown source: " + leftSourceName));

      throwOnIncompleteJoinCriteria(left, right, leftSourceName, rightSourceName);
      throwOnIncompatibleSourceWindowing(left, right);
      throwOnJoinWithoutSource(source, left, right);

      // we consider this JOIN "flipped" if the left hand side of the expression
      // is not the source - in the case of multi-way joins, the JoinInfo will be
      // flipped in the JoinTree so that the new source is always on the right and
      // the result of previous joins is on the left
      final boolean flipped = rightSourceName.equals(analysis.getFrom().getAlias());
      final JoinInfo joinInfo = new JoinInfo(
          left,
          comparisonExpression.getLeft(),
          right,
          comparisonExpression.getRight(),
          joinType,
          node.getWithinExpression()
      );
      analysis.addJoin(flipped ? joinInfo.flip() : joinInfo);

      return null;
    }

    private void throwOnJoinWithoutSource(
            final AliasedDataSource source,
            final AliasedDataSource left,
            final AliasedDataSource right
    ) {
      if (!source.equals(left) && !source.equals(right)) {
        throw new KsqlException(
            "A join criteria is expected to reference the source (" + source.getAlias()
                + ") in the FROM clause, instead the right source references " + right.getAlias()
                + " and the left source references " + left.getAlias()
        );
      }
    }

    private void throwOnIncompleteJoinCriteria(
        final AliasedDataSource left,
        final AliasedDataSource right,
        final SourceName leftExpressionSource,
        final SourceName rightExpressionSource
    ) {
      final ImmutableSet<SourceName> usedSources = ImmutableSet
          .of(leftExpressionSource, rightExpressionSource);
      final boolean valid = usedSources.size() == 2
          && usedSources.containsAll(ImmutableList.of(left.getAlias(), right.getAlias()));

      if (!valid) {
        throw new KsqlException(
            "Each side of the join must reference exactly one source and not the same source. "
                + "Left side references " + leftExpressionSource
                + " and right references " + rightExpressionSource
        );
      }
    }

    private void throwOnIncompatibleSourceWindowing(
        final AliasedDataSource left,
        final AliasedDataSource right
    ) {
      final Optional<WindowType> leftWindowType = left.getDataSource()
          .getKsqlTopic()
          .getKeyFormat()
          .getWindowInfo()
          .map(WindowInfo::getType);

      final Optional<WindowType> rightWindowType = right.getDataSource()
          .getKsqlTopic()
          .getKeyFormat()
          .getWindowInfo()
          .map(WindowInfo::getType);

      if (leftWindowType.isPresent() != rightWindowType.isPresent()) {
        throw windowedNonWindowedJoinException(left, right, leftWindowType, rightWindowType);
      }

      if (!leftWindowType.isPresent()) {
        return;
      }

      final WindowType leftWt = leftWindowType.get();
      final WindowType rightWt = rightWindowType.get();
      final boolean compatible = leftWt == WindowType.SESSION
          ? rightWt == WindowType.SESSION
          : rightWt == WindowType.HOPPING || rightWt == WindowType.TUMBLING;

      if (!compatible) {
        throw new KsqlException("Incompatible windowed sources."
            + System.lineSeparator()
            + "Left source: " + leftWt
            + System.lineSeparator()
            + "Right source: " + rightWt
            + System.lineSeparator()
            + "Session windowed sources can only be joined to other session windowed sources, "
            + "and may still not result in expected behaviour as session bounds must be an exact "
            + "match for the join to work"
            + System.lineSeparator()
            + "Hopping and tumbling windowed sources can only be joined to other hopping and "
            + "tumbling windowed sources"
        );
      }
    }

    private KsqlException windowedNonWindowedJoinException(
        final AliasedDataSource left,
        final AliasedDataSource right,
        final Optional<WindowType> leftWindowType,
        final Optional<WindowType> rightWindowType
    ) {
      final String leftMsg = leftWindowType.map(Object::toString).orElse("not");
      final String rightMsg = rightWindowType.map(Object::toString).orElse("not");
      return new KsqlException("Can not join windowed source to non-windowed source."
          + System.lineSeparator()
          + left.getAlias() + " is " + leftMsg + " windowed"
          + System.lineSeparator()
          + right.getAlias() + " is " + rightMsg + " windowed"
      );
    }

    private SourceName getOnlySourceForJoin(
        final Expression exp,
        final ComparisonExpression join,
        final Set<SourceName> sources
    ) {
      try {
        return Iterables.getOnlyElement(sources);
      } catch (final Exception e) {
        throw new KsqlException("Invalid comparison expression '" + exp + "' in join '" + join
            + "'. Each side of the join comparision must contain references from exactly one "
            + "source.");
      }
    }

    private JoinNode.JoinType getJoinType(final JoinedSource source) {
      final JoinNode.JoinType joinType;
      switch (source.getType()) {
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
          throw new KsqlException("Join type is not supported: " + source.getType().name());
      }
      return joinType;
    }

    @Override
    protected AstNode visitAliasedRelation(final AliasedRelation node, final Void context) {
      final SourceName sourceName = ((Table) node.getRelation()).getName();

      final DataSource source = metaStore.getSource(sourceName);
      if (source == null) {
        throw new UnknownSourceException(Optional.empty(), sourceName);
      }

      final Optional<AliasedDataSource> existing = analysis.getSourceByName(source.getName());
      if (existing.isPresent()) {
        throw new KsqlException(
            "Can not join '"
                + sourceName.toString(FormatOptions.noEscape())
                + "' to '"
                + existing.get().getDataSource().getName().toString(FormatOptions.noEscape())
                + "': self joins are not yet supported."
        );
      }
      analysis.addDataSource(node.getAlias(), source);
      return node;
    }

    @Override
    protected AstNode visitSelect(final Select node, final Void context) {
      for (final SelectItem selectItem : node.getSelectItems()) {
        analysis.addSelectItem(selectItem);

        if (selectItem instanceof SingleColumn) {
          final SingleColumn column = (SingleColumn) selectItem;
          validateSelect(column);
          captureReferencedSourceColumns(column.getExpression());
          visitTableFunctions(column.getExpression());
        } else if (!(selectItem instanceof AllColumns)) {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + selectItem.getClass().getName());
        }
      }
      return null;
    }

    private void analyzeWhere(final Expression node) {
      analysis.setWhereExpression(node);
    }

    private void analyzeGroupBy(final GroupBy groupBy) {
      isGroupBy = true;
      analysis.setGroupBy(groupBy);
    }

    private void analyzePartitionBy(final PartitionBy partitionBy) {
      analysis.setPartitionBy(partitionBy);
    }

    private void analyzeWindowExpression(final WindowExpression windowExpression) {
      analysis.setWindowExpression(windowExpression);
    }

    private void analyzeHaving(final Expression node) {
      analysis.setHavingExpression(node);
    }

    private void validateSelect(final SingleColumn column) {
      final ColumnName columnName = column.getAlias()
          .orElseThrow(IllegalStateException::new);

      if (persistent) {
        if (SystemColumns.isSystemColumn(columnName)) {
          throw new KsqlException("Reserved column name in select: " + columnName + ". "
              + "Please remove or alias the column.");
        }
      }

      if (!analysis.getGroupBy().isPresent()) {
        throwOnUdafs(column.getExpression());
      }
    }

    private void throwOnUdafs(final Expression expression) {
      new TraversalExpressionVisitor<Void>() {
        @Override
        public Void visitFunctionCall(final FunctionCall functionCall, final Void context) {
          final FunctionName functionName = functionCall.getName();
          if (metaStore.isAggregate(functionName)) {
            throw new KsqlException("Use of aggregate function "
                + functionName.text() + " requires a GROUP BY clause.");
          }

          super.visitFunctionCall(functionCall, context);
          return null;
        }
      }.process(expression, null);
    }

    public void validate() {
      final String kafkaSources = analysis.getAllDataSources().stream()
          .filter(s -> s.getDataSource().getKsqlTopic().getValueFormat().getFormat()
              == FormatFactory.KAFKA)
          .map(AliasedDataSource::getAlias)
          .map(SourceName::text)
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

    private void captureReferencedSourceColumns(final Expression exp) {
      final List<ColumnName> columnNames = ColumnExtractor.extractColumns(exp).stream()
          .map(ColumnReferenceExp::getColumnName)
          .collect(Collectors.toList());

      analysis.addSelectColumnRefs(columnNames);
    }

    private void visitTableFunctions(final Expression expression) {
      final TableFunctionVisitor visitor = new TableFunctionVisitor();
      visitor.process(expression, null);
    }

    private final class TableFunctionVisitor extends TraversalExpressionVisitor<Void> {

      private Optional<FunctionName> tableFunctionName = Optional.empty();

      @Override
      public Void visitFunctionCall(final FunctionCall functionCall, final Void context) {
        final FunctionName functionName = functionCall.getName();
        final boolean isTableFunction = metaStore.isTableFunction(functionName);

        if (isTableFunction) {
          if (tableFunctionName.isPresent()) {
            throw new KsqlException("Table functions cannot be nested: "
                + tableFunctionName.get() + "(" + functionName + "())");
          }

          tableFunctionName = Optional.of(functionName);

          if (analysis.getGroupBy().isPresent()) {
            throw new KsqlException("Table functions cannot be used with aggregations.");
          }

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
}
