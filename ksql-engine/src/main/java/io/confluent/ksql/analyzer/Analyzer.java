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
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.DefaultTraversalVisitor;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class Analyzer {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Set<String> VALID_WITH_PROPS = ImmutableSet.<String>builder()
      .add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase())
      .add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase())
      .add(DdlConfig.PARTITION_BY_PROPERTY.toUpperCase())
      .add(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME.toUpperCase())
      .add(KsqlConstants.SINK_NUMBER_OF_PARTITIONS.toUpperCase())
      .add(KsqlConstants.SINK_NUMBER_OF_REPLICAS.toUpperCase())
      .add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase())
      .add(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME.toUpperCase())
      .add(DdlConfig.WRAP_SINGLE_VALUE.toUpperCase())
      .build();

  private final MetaStore metaStore;
  private final String topicPrefix;
  private final SerdeFactories serdeFactories;
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
        new KsqlSerdeFactories(),
        SerdeOptions::buildForCreateAsStatement);
  }

  @VisibleForTesting
  Analyzer(
      final MetaStore metaStore,
      final String topicPrefix,
      final Set<SerdeOption> defaultSerdeOptions,
      final SerdeFactories serdeFactories,
      final SerdeOptionsSupplier serdeOptionsSupplier
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix");
    this.defaultSerdeOptions = ImmutableSet
        .copyOf(requireNonNull(defaultSerdeOptions, "defaultSerdeOptions"));
    this.serdeFactories = requireNonNull(serdeFactories, "serdeFactories");
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

    return visitor.analysis;
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private final class Visitor extends DefaultTraversalVisitor<Node, Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final Analysis analysis = new Analysis();
    private Optional<String> intoKafkaTopicName = Optional.empty();

    private void analyzeSink(
        final Optional<Sink> sink,
        final String sqlExpression
    ) {
      sink.ifPresent(s -> analyzeNonStdOutSink(s, sqlExpression));
    }

    private void setIntoProperties(final Sink sink) {
      validateWithClause(sink.getProperties().keySet());

      setIntoTopicName(sink.getProperties());

      setPartitionBy(sink.getProperties());

      setIntoTimestampColumnAndFormat(sink.getProperties());

      setSerdeOptions(sink);
    }

    private void setPartitionBy(final Map<String, Expression> properties) {
      if (properties.get(DdlConfig.PARTITION_BY_PROPERTY) == null) {
        return;
      }

      final String intoPartitionByColumnName = properties
          .get(DdlConfig.PARTITION_BY_PROPERTY)
          .toString()
          .toUpperCase();

      analysis.setPartitionBy(intoPartitionByColumnName);
    }

    private void analyzeNonStdOutSink(
        final Sink sink,
        final String sqlExpression
    ) {
      setIntoProperties(sink);

      if (!sink.shouldCreateSink()) {
        final DataSource<?> existing = metaStore.getSource(sink.getName());
        if (existing == null) {
          throw new KsqlException("Unknown source: " + sink.getName());
        }

        if (metaStore.getTopic(sink.getName()) == null) {
          throw new KsqlException(
              "Sink topic " + sink.getName() + " does not exist in the metastore.");
        }

        analysis.setInto(Into.of(
            sqlExpression,
            sink.getName(),
            false,
            existing.getKsqlTopic(),
            existing.getKeySerdeFactory()
        ));
        return;
      }

      final String topicName = intoKafkaTopicName
          .orElseGet(() -> topicPrefix + sink.getName());

      final KsqlSerdeFactory valueSerdeFactory = getValueSerdeFactory(sink);

      final KsqlTopic intoKsqlTopic = new KsqlTopic(
          sink.getName(),
          topicName,
          valueSerdeFactory,
          true
      );

      analysis.setInto(Into.of(
          sqlExpression,
          sink.getName(),
          true,
          intoKsqlTopic,
          Serdes::String
      ));
    }

    private KsqlSerdeFactory getValueSerdeFactory(final Sink sink) {
      final Format format = getValueFormat(sink);
      return serdeFactories.create(format, sink.getProperties());
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
      final Object serdeProperty = sink.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY);

      if (serdeProperty != null) {
        return Format.of(StringUtil.cleanQuotes(serdeProperty.toString()));
      }

      final DataSource<?> leftSource = analysis.getFromDataSources().get(0).getDataSource();
      return leftSource.getKsqlTopic().getValueSerdeFactory().getFormat();
    }

    private void setIntoTopicName(final Map<String, Expression> properties) {
      final Expression expression = properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);
      if (expression == null) {
        intoKafkaTopicName = Optional.empty();
        return;
      }

      if (!(expression instanceof StringLiteral)) {
        throw new KsqlException(
            DdlConfig.KAFKA_TOPIC_NAME_PROPERTY + " should be a string literal.");
      }

      final String kafkaTopicName = ((StringLiteral) expression).getValue();
      intoKafkaTopicName = Optional.of(kafkaTopicName);
    }

    private void setIntoTimestampColumnAndFormat(final Map<String, Expression> properties) {
      final Expression columnNameExp = properties.get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME);
      if (columnNameExp != null) {
        if (!(columnNameExp instanceof Literal)) {
          throw new KsqlException(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME
              + " set in the WITH clause must be set to a literal");
        }

        analysis.setTimestampColumnName(((Literal) columnNameExp).getValue().toString());
      }

      final Expression formatExp = properties.get(DdlConfig.TIMESTAMP_FORMAT_PROPERTY);
      if (formatExp != null) {
        if (columnNameExp == null) {
          throw new KsqlException("You must specify a " + KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME
              + " if you wish to specify " + DdlConfig.TIMESTAMP_FORMAT_PROPERTY);
        }

        if (!(formatExp instanceof Literal)) {
          throw new KsqlException(DdlConfig.TIMESTAMP_FORMAT_PROPERTY
              + " set in the WITH clause must be set to a literal");
        }

        analysis.setTimestampFormat(((Literal) formatExp).getValue().toString());
      }
    }

    private void setSerdeOptions(final Sink sink) {
      final List<String> columnNames = getNoneMetaOrKeySelectAliases();

      final Format valueFormat = getValueFormat(sink);

      final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
          columnNames,
          sink.getProperties(),
          valueFormat,
          defaultSerdeOptions
      );

      analysis.setSerdeOptions(serdeOptions);
    }

    private void validateWithClause(final Set<String> withClauseVariables) {
      for (final String withVariable : withClauseVariables) {
        if (!VALID_WITH_PROPS.contains(withVariable.toUpperCase())) {
          throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
        }
      }
    }

    @Override
    protected Node visitQuery(
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
    protected Node visitJoin(final Join node, final Void context) {
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
    protected Node visitAliasedRelation(final AliasedRelation node, final Void context) {
      final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();

      final DataSource<?> source = metaStore.getSource(structuredDataSourceName);
      if (source == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      analysis.addDataSource(node.getAlias(), source);
      return node;
    }

    @Override
    protected Node visitCast(final Cast node, final Void context) {
      return process(node.getExpression(), context);
    }

    @Override
    protected Node visitSelect(final Select node, final Void context) {
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
    protected Node visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Void context
    ) {
      return visitExpression(node, context);
    }

    @Override
    protected Node visitGroupBy(final GroupBy node, final Void context) {
      return null;
    }

    private void analyzeWhere(final Node node) {
      analysis.setWhereExpression((Expression) node);
    }

    private void analyzeGroupBy(final GroupBy groupBy) {
      for (final GroupingElement groupingElement : groupBy.getGroupingElements()) {
        final Set<Expression> groupingSet = groupingElement.enumerateGroupingSets().get(0);
        analysis.addGroupByExpressions(groupingSet);
      }
    }

    private void analyzeWindowExpression(final WindowExpression windowExpression) {
      analysis.setWindowExpression(windowExpression);
    }

    private void analyzeHaving(final Node node) {
      analysis.setHavingExpression((Expression) node);
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
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        List<String> columnNames,
        Map<String, Expression> properties,
        Format valueFormat,
        Set<SerdeOption> singleFieldDefaults
    );
  }
}
