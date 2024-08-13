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
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.BytesLiteral;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.streams.PartitionByParamsFactory;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
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
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.UnknownColumnException;
import io.confluent.ksql.util.UnknownSourceException;
import java.nio.ByteBuffer;
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
  private final boolean pullLimitClauseEnabled;

  /**
   * @param metaStore the metastore to use.
   * @param topicPrefix the prefix to use for topic names where an explicit name is not specified.
   */
  Analyzer(
      final MetaStore metaStore,
      final String topicPrefix,
      final boolean pullLimitClauseEnabled

  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix");
    this.pullLimitClauseEnabled = pullLimitClauseEnabled;
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
      this.analysis = new Analysis(
              query.getRefinement(),
              pullLimitClauseEnabled
          );

      this.persistent = persistent;
    }

    private void analyzeNonStdOutSink(final Sink sink) {
      final CreateSourceAsProperties props = sink.getProperties();

      analysis.setProperties(props);

      if (!sink.shouldCreateSink()) {
        final DataSource existing = metaStore.getSource(sink.getName());
        if (existing == null) {
          throw new KsqlException("Unknown source: "
              + sink.getName().toString(FormatOptions.noEscape()));
        }

        analysis.setInto(Into.existingSink(sink.getName(), existing.getKsqlTopic()));
        return;
      }

      final String topicName = props.getKafkaTopic()
          .orElseGet(() -> topicPrefix + sink.getName().text());

      final KsqlTopic srcTopic = analysis
          .getFrom()
          .getDataSource()
          .getKsqlTopic();

      final String keyFormatName = keyFormatName(
          props.getKeyFormat(),
          srcTopic.getKeyFormat().getFormatInfo()
      );
      final FormatInfo keyFmtInfo = buildFormatInfo(
          keyFormatName,
          props.getKeyFormatProperties(sink.getName().text(), keyFormatName),
          srcTopic.getKeyFormat().getFormatInfo()
      );

      final String valueFormatName = formatName(
          props.getValueFormat(),
          srcTopic.getValueFormat().getFormatInfo()
      );
      final FormatInfo valueFmtInfo = buildFormatInfo(
          valueFormatName,
          props.getValueFormatProperties(valueFormatName),
          srcTopic.getValueFormat().getFormatInfo()
      );

      final Optional<WindowInfo> explicitWindowInfo = analysis.getWindowExpression()
          .map(WindowExpression::getKsqlWindowExpression)
          .map(KsqlWindowExpression::getWindowInfo);

      final Optional<WindowInfo> windowInfo = explicitWindowInfo.isPresent()
          ? explicitWindowInfo
          : srcTopic.getKeyFormat().getWindowInfo();

      analysis
          .setInto(Into.newSink(sink.getName(), topicName, windowInfo, keyFmtInfo, valueFmtInfo));

      analysis.setOrReplace(sink.shouldReplace());
    }

    private String keyFormatName(
        final Optional<String> explicitFormat,
        final FormatInfo sourceFormat
    ) {
      final boolean partitioningByNull = analysis.getPartitionBy()
          .map(pb -> PartitionByParamsFactory.isPartitionByNull(pb.getExpressions()))
          .orElse(false);
      if (partitioningByNull) {
        final boolean nonNoneExplicitFormat = explicitFormat
            .map(fmt -> !fmt.equalsIgnoreCase(NoneFormat.NAME))
            .orElse(false);

        if (nonNoneExplicitFormat) {
          throw new KsqlException("Key format specified for stream without key columns.");
        }

        return NoneFormat.NAME;
      }

      return formatName(explicitFormat, sourceFormat);
    }

    private String formatName(
        final Optional<String> explicitFormat,
        final FormatInfo sourceFormat
    ) {
      return explicitFormat.orElse(sourceFormat.getFormat());
    }

    private FormatInfo buildFormatInfo(
        final String formatName,
        final Map<String, String> formatProperties,
        final FormatInfo sourceFormat
    ) {
      final Format format = FormatFactory.fromName(formatName);

      final Map<String, String> props = new HashMap<>();
      if (formatName.equals(sourceFormat.getFormat())) {
        sourceFormat.getProperties().forEach((k, v) -> {
          if (format.getInheritableProperties().contains(k)) {
            props.put(k, v);
          }
        });
      }

      // overwrite any inheritable properties if they were explicitly
      // specified in the statement
      props.putAll(formatProperties);

      return FormatInfo.of(formatName, props);
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

      try {
        analysis.getPartitionBy()
            .map(PartitionBy::getExpressions)
            .orElseGet(ImmutableList::of)
            .forEach(expression -> columnValidator.analyzeExpression(expression, "PARTITION BY"));
      } catch (final UnknownColumnException e) {
        throw new UnknownColumnException(
            e.getPrefix(),
            e.getColumnExp(),
            "cannot be resolved. '"
                + e.getColumnExp() + "' must be a column in the source schema since PARTITION BY"
                + " is applied on the input.");
      }

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
      final Expression joinExp = joinOn.getExpression();
      if (!(joinExp instanceof ComparisonExpression)) {
        // add in a special check for multi-column joins so that we can throw
        // an even more useful error message
        if (joinExp instanceof LogicalBinaryExpression
            && isEqualityJoin(((LogicalBinaryExpression) joinExp).getLeft())
            && isEqualityJoin(((LogicalBinaryExpression) joinExp).getRight())) {
          throw new KsqlException(String.format(
              "Invalid join condition: joins on multiple conditions are not yet supported. Got %s.",
              joinExp
          ));
        }

        throw new KsqlException("Unsupported join expression: " + joinExp);
      }
      final ComparisonExpression comparisonExpression = (ComparisonExpression) joinExp;

      if (!(isEqualityJoin(joinExp))) {
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
          flipped,
          node.getWithinExpression()
      );
      analysis.addJoin(flipped ? joinInfo.flip() : joinInfo);

      return null;
    }

    private boolean isEqualityJoin(final Expression exp) {
      return exp instanceof ComparisonExpression
          && ((ComparisonExpression) exp).getType() == Type.EQUAL;
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
        case RIGHT:
          joinType = JoinNode.JoinType.RIGHT;
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
        final String errorMsg = analysis.getAllDataSources().size() > 1
            ? "N-way joins do not support multiple occurrences of the same source. "
                + "Source: '" + sourceName.toString(FormatOptions.noEscape()) + "'."
            : "Can not join '"
                + sourceName.toString(FormatOptions.noEscape())
                + "' to '"
                + existing.get().getDataSource().getName().toString(FormatOptions.noEscape())
                + "': self joins are not yet supported.";
        throw new KsqlException(errorMsg);
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
        } else if (selectItem instanceof StructAll) {
          final StructAll structAll = (StructAll) selectItem;
          captureReferencedSourceColumns(structAll.getBaseStruct());
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

      SystemColumns.systemColumnNames()
          .forEach(col -> checkForReservedToken(column, col));

      if (!analysis.getGroupBy().isPresent()) {
        addDummyGroupbyForUdafs(column.getExpression());
      }
    }

    private void checkForReservedToken(
        final SingleColumn singleColumn,
        final ColumnName reservedToken
    ) {
      final ColumnName alias = singleColumn.getAlias().orElseThrow(IllegalStateException::new);
      final Expression expression = singleColumn.getExpression();

      if (alias.text().equalsIgnoreCase(reservedToken.text())) {

        //if a column's alias matches a reserved token but not the expression text, it means that
        //the user has explicitly tried to alias a column as a reserved token, so throw this message
        if (!expressionMatchesAlias(expression, alias)) {
          throw new KsqlException("`" + reservedToken.text() + "` "
              + "is a reserved column name. "
              + "You cannot use it as an alias for a column.");

        //if an unaliased column matches a reserved token (ie a user issued SELECT ROWTIME FROM x)
        //we can't allow the query if it is persistent. If it is transient, allow it.
        } else if (persistent) {
          throw new KsqlException("Reserved column name in select: "
              + "`" + reservedToken.text() + "`. "
              + "Please remove or alias the column.");
        }

      }
    }

    private boolean expressionMatchesAlias(final Expression expression, final ColumnName alias) {
      final String text = expression.toString();
      final String unqualifiedExpression = text.substring(text.indexOf(".") + 1);
      return unqualifiedExpression.equalsIgnoreCase(alias.text());
    }

    private void addDummyGroupbyForUdafs(final Expression expression) {
      new TraversalExpressionVisitor<Void>() {
        @Override
        public Void visitFunctionCall(final FunctionCall functionCall, final Void context) {
          final FunctionName functionName = functionCall.getName();
          if (metaStore.isAggregate(functionName)) {
            analysis.addAggregateFunction(functionCall);
            // Since this is a dummy group by, we don't actually need a correct node location
            analysis.setGroupBy(new GroupBy(Optional.empty(),
                ImmutableList.of(new BytesLiteral(ByteBuffer.wrap(new byte[] {1})))));
          }

          super.visitFunctionCall(functionCall, context);
          return null;
        }
      }.process(expression, null);
    }

    public void validate() {
      final String kafkaSources = analysis.getAllDataSources().stream()
          .filter(s -> s.getDataSource().getKsqlTopic().getValueFormat().getFormat()
                  .equals(KafkaFormat.NAME))
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
      private Optional<SearchedCaseExpression> searchedCaseExpression = Optional.empty();

      @Override
      public Void visitSearchedCaseExpression(
          final SearchedCaseExpression node,
          final Void context
      ) {
        searchedCaseExpression = Optional.of(node);
        super.visitSearchedCaseExpression(node, context);
        return null;
      }

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

          if (searchedCaseExpression.isPresent()) {
            throw new KsqlException("Table functions cannot be used in CASE: "
                + ExpressionFormatter.formatExpression(searchedCaseExpression.get()));
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
