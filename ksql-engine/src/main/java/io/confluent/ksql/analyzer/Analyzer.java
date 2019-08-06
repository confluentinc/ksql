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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
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
   * @param sqlExpression the sql expression being analysed.
   * @param query the query to analyze.
   * @param sink the sink the query will output to.
   * @return the analysis.
   */
  Analysis analyze(
      final String sqlExpression,
      final Query query,
      final Optional<Sink> sink
  ) {
    final Visitor visitor = new Visitor();
    visitor.process(query, null);

    visitor.analyzeSink(sink, sqlExpression);

    visitor.validate();

    return visitor.analysis;
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private final class Visitor extends DefaultTraversalVisitor<AstNode, Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final Analysis analysis = new Analysis();
    private boolean isJoin = false;
    private boolean isGroupBy = false;

    private void analyzeSink(
        final Optional<Sink> sink,
        final String sqlExpression
    ) {
      sink.ifPresent(s -> analyzeNonStdOutSink(s, sqlExpression));
    }

    private void analyzeNonStdOutSink(
        final Sink sink,
        final String sqlExpression
    ) {
      analysis.setProperties(sink.getProperties());
      sink.getPartitionBy().ifPresent(analysis::setPartitionBy);

      setSerdeOptions(sink);

      if (!sink.shouldCreateSink()) {
        final DataSource<?> existing = metaStore.getSource(sink.getName());
        if (existing == null) {
          throw new KsqlException("Unknown source: " + sink.getName());
        }

        analysis.setInto(Into.of(
            sqlExpression,
            sink.getName(),
            false,
            existing.getKsqlTopic()
        ));
        return;
      }

      final String topicName = sink.getProperties().getKafkaTopic()
          .orElseGet(() -> topicPrefix + sink.getName());

      final KeyFormat keyFormat = buildKeyFormat();

      final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(
          getValueFormat(sink),
          sink.getProperties().getValueAvroSchemaName()
      ));

      final KsqlTopic intoKsqlTopic = new KsqlTopic(
          topicName,
          keyFormat,
          valueFormat,
          true
      );

      analysis.setInto(Into.of(
          sqlExpression,
          sink.getName(),
          true,
          intoKsqlTopic
      ));
    }

    private KeyFormat buildKeyFormat() {
      final Optional<KsqlWindowExpression> ksqlWindow = Optional
          .ofNullable(analysis.getWindowExpression())
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
      final List<String> columnNames = getNoneMetaOrKeySelectAliases();

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
     * Get the list of select expressions that are <i>not</i> for meta and key fields in the source
     * schema.
     *
     * <p>Currently, the select expressions can include metadata and key fields, which are later
     * removed by {@link io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode}. When building
     * the {@link SerdeOptions} its important the final schema is used, i.e. the one with these
     * meta and key fields removed.
     *
     * <p>This is weird functionality, but maintained for backwards compatibility for now.
     *
     * @return the list of field names in the sink that are not meta or key fields.
     */
    private List<String> getNoneMetaOrKeySelectAliases() {
      final SourceSchemas sourceSchemas = analysis.getFromSourceSchemas();
      final List<Expression> selects = analysis.getSelectExpressions();

      final List<String> columnNames = new ArrayList<>(analysis.getSelectExpressionAlias());

      for (int idx = selects.size() - 1; idx >= 0; --idx) {
        final Expression select = selects.get(idx);

        if (!(select instanceof DereferenceExpression)
            && !(select instanceof QualifiedNameReference)) {
          continue;
        }

        if (!sourceSchemas.matchesNonValueField(select.toString())) {
          continue;
        }

        final String columnName = columnNames.get(idx);
        if (columnName.equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
            || columnName.equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
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

      for (final Expression selectExpression : analysis.getSelectExpressions()) {
        expressionAnalyzer.analyzeExpression(selectExpression);
      }

      if (analysis.getWhereExpression() != null) {
        expressionAnalyzer.analyzeExpression(analysis.getWhereExpression());
      }

      for (final Expression expression : analysis.getGroupByExpressions()) {
        expressionAnalyzer.analyzeExpression(expression);
      }

      if (analysis.getHavingExpression() != null) {
        expressionAnalyzer.analyzeExpression(analysis.getHavingExpression());
      }
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

      final String leftJoinField = getJoinFieldName(
          comparisonExpression,
          left.getAlias(),
          left.getDataSource().getSchema()
      );

      final String rightJoinField = getJoinFieldName(
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

    private String getJoinFieldName(
        final ComparisonExpression comparisonExpression,
        final String sourceAlias,
        final LogicalSchema sourceSchema
    ) {
      Optional<String> joinField = getJoinFieldFromExpr(
          comparisonExpression.getLeft(),
          sourceAlias,
          sourceSchema
      );

      if (!joinField.isPresent()) {
        joinField = getJoinFieldFromExpr(
            comparisonExpression.getRight(),
            sourceAlias,
            sourceSchema
        );
      }

      return joinField
          .orElseThrow(() -> new KsqlException(
              String.format(
                  "%s : Invalid join criteria %s. Could not find a join criteria operand for %s. ",
                  comparisonExpression.getLocation().map(Objects::toString).orElse(""),
                  comparisonExpression, sourceAlias
              )
          ));
    }

    private Optional<String> getJoinFieldFromExpr(
        final Expression expression,
        final String sourceAlias,
        final LogicalSchema sourceSchema
    ) {
      if (expression instanceof DereferenceExpression) {
        final DereferenceExpression dereferenceExpr = (DereferenceExpression) expression;

        final String sourceAliasVal = dereferenceExpr.getBase().toString();
        if (!sourceAliasVal.equalsIgnoreCase(sourceAlias)) {
          return Optional.empty();
        }

        final String fieldName = dereferenceExpr.getFieldName();
        return getJoinFieldFromSource(fieldName, sourceAlias, sourceSchema);
      }

      if (expression instanceof QualifiedNameReference) {
        final QualifiedNameReference qualifiedNameRef = (QualifiedNameReference) expression;

        final String fieldName = qualifiedNameRef.getName().getSuffix();
        return getJoinFieldFromSource(fieldName, sourceAlias, sourceSchema);
      }
      return Optional.empty();
    }

    private Optional<String> getJoinFieldFromSource(
        final String fieldName,
        final String sourceAlias,
        final LogicalSchema sourceSchema
    ) {
      return sourceSchema.findField(fieldName)
          .map(field -> SchemaUtil.buildAliasedFieldName(sourceAlias, field.name()));
    }

    @Override
    protected AstNode visitAliasedRelation(final AliasedRelation node, final Void context) {
      final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();

      final DataSource<?> source = metaStore.getSource(structuredDataSourceName);
      if (source == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      analysis.addDataSource(node.getAlias(), source);
      return node;
    }

    @Override
    public AstNode visitCast(final Cast node, final Void context) {
      return process(node.getExpression(), context);
    }

    @Override
    protected AstNode visitSelect(final Select node, final Void context) {
      for (final SelectItem selectItem : node.getSelectItems()) {
        if (selectItem instanceof AllColumns) {
          visitSelectStar((AllColumns) selectItem);
        } else if (selectItem instanceof SingleColumn) {
          final SingleColumn column = (SingleColumn) selectItem;
          analysis.addSelectItem(column.getExpression(), column.getAlias());
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + selectItem.getClass().getName());
        }
      }
      return null;
    }

    @Override
    public AstNode visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Void context
    ) {
      return visitExpression(node, context);
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

      final String prefix = allColumns.getPrefix()
          .map(QualifiedName::toString)
          .orElse("");

      for (final AliasedDataSource source : analysis.getFromDataSources()) {

        if (!prefix.isEmpty() && !prefix.equals(source.getAlias())) {
          continue;
        }

        final QualifiedName name = QualifiedName.of(source.getAlias());

        final QualifiedNameReference nameRef = new QualifiedNameReference(location, name);

        final String aliasPrefix = analysis.isJoin()
            ? source.getAlias() + "_"
            : "";

        for (final Field field : source.getDataSource().getSchema().fields()) {

          final DereferenceExpression selectItem =
              new DereferenceExpression(location, nameRef, field.name());

          final String alias = aliasPrefix + field.name();

          analysis.addSelectItem(selectItem, alias);
        }
      }
    }

    public void validate() {
      final String kafkaSources = analysis.getFromDataSources().stream()
          .filter(s -> s.getDataSource().getKsqlTopic().getValueFormat().getFormat()
              == Format.KAFKA)
          .map(AliasedDataSource::getAlias)
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
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        List<String> valueColumnNames,
        Format valueFormat,
        Optional<Boolean> wrapSingleValues,
        Set<SerdeOption> singleFieldDefaults
    );
  }
}
