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

import static java.lang.String.format;

import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
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
import io.confluent.ksql.parser.tree.Node;
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
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.WithClauseUtil;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class Analyzer {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final String sqlExpression;
  private final Analysis analysis;
  private final MetaStore metaStore;
  private final String topicPrefix;

  /**
   * @param sqlExpression the sql expression to analyse
   * @param analysis      where the results are stored.
   * @param metaStore     the metastore to use.
   * @param topicPrefix   the prefix to use for topic names where an explicit name is not specified.
   */
  Analyzer(
      final String sqlExpression,
      final Analysis analysis,
      final MetaStore metaStore,
      final String topicPrefix
  ) {
    this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.topicPrefix = Objects.requireNonNull(topicPrefix, "topicPrefix");
  }

  /**
   * Analyze the query.
   *
   * @param query the query to analyze.
   * @param sink the sink the query will output to.
   */
  void analyze(final Query query, final Optional<Sink> sink) {
    new Visitor().process(query, null);

    analyzeSink(sink);
  }

  private void analyzeSink(final Optional<Sink> sink) {
    sink.ifPresent(this::analyzeNonStdOutSink);
  }

  private void setIntoProperties(final Sink sink) {

    validateWithClause(sink.getProperties().keySet());

    setIntoTopicFormat(sink);

    if (sink.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
      setIntoTopicName(sink);
    }

    if (sink.getProperties().get(DdlConfig.PARTITION_BY_PROPERTY) != null) {
      final String intoPartitionByColumnName = sink.getProperties()
          .get(DdlConfig.PARTITION_BY_PROPERTY)
          .toString()
          .toUpperCase();
      analysis.getIntoProperties().put(
          DdlConfig.PARTITION_BY_PROPERTY,
          intoPartitionByColumnName
      );
    }

    if (sink.getProperties().get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME) != null) {
      setIntoTimestampColumnAndFormat(sink);
    }

    if (sink.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS) != null) {
      final int numberOfPartitions =
          WithClauseUtil.parsePartitions(
              sink.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS).toString());

      analysis.getIntoProperties().put(
          KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
          numberOfPartitions
      );
    }

    if (sink.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS) != null) {
      final short numberOfReplications =
          WithClauseUtil.parseReplicas(
              sink.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS).toString());
      analysis.getIntoProperties()
          .put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, numberOfReplications);
    }
  }

  private void analyzeNonStdOutSink(final Sink sink) {
    setIntoProperties(sink);

    if (!sink.shouldCreateSink()) {
      final StructuredDataSource<?> existing = metaStore.getSource(sink.getName());
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

    final String intoKafkaTopicName = analysis.getIntoKafkaTopicName() == null
        ? topicPrefix + sink.getName()
        : analysis.getIntoKafkaTopicName();

    final KsqlTopic intoKsqlTopic = new KsqlTopic(
        sink.getName(),
        intoKafkaTopicName,
        getIntoValueSerde(),
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

  private KsqlTopicSerDe getIntoValueSerde() {
    final List<Pair<StructuredDataSource, String>> fromDataSources = analysis
        .getFromDataSources();

    if (analysis.getIntoFormat() != null) {
      switch (analysis.getIntoFormat().toUpperCase()) {
        case DataSource.AVRO_SERDE_NAME:
          final String schemaFullName =
              StringUtil.cleanQuotes(
                  analysis.getIntoProperties().get(
                      DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME).toString());
          return new KsqlAvroTopicSerDe(schemaFullName);

        case DataSource.JSON_SERDE_NAME:
          return new KsqlJsonTopicSerDe();

        case DataSource.DELIMITED_SERDE_NAME:
          return new KsqlDelimitedTopicSerDe();

        default:
          throw new KsqlException(
              String.format("Unsupported format: %s", analysis.getIntoFormat()));
      }
    }

    final KsqlTopicSerDe intoTopicSerde = fromDataSources.get(0)
        .getLeft()
        .getKsqlTopic()
        .getKsqlTopicSerDe();

    if (intoTopicSerde instanceof KsqlAvroTopicSerDe) {
      final String schemaFullName = StringUtil.cleanQuotes(
              analysis.getIntoProperties().get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME).toString());
      return new KsqlAvroTopicSerDe(schemaFullName);
    }
    return intoTopicSerde;
  }

  private void setIntoTopicName(final Sink sink) {
    String intoKafkaTopicName =
        sink.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    if (!intoKafkaTopicName.startsWith("'") && !intoKafkaTopicName.endsWith("'")) {
      throw new KsqlException(
          intoKafkaTopicName + " value is string and should be enclosed between " + "\"'\".");
    }
    intoKafkaTopicName = intoKafkaTopicName.substring(1, intoKafkaTopicName.length() - 1);
    analysis.setIntoKafkaTopicName(intoKafkaTopicName);
    analysis.getIntoProperties().put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, intoKafkaTopicName);
  }

  private void setIntoTopicFormat(final Sink sink) {
    final Object serdeProperty = sink.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY);

    String serde;
    if (serdeProperty != null) {
      serde = serdeProperty.toString();

      if (!serde.startsWith("'") && !serde.endsWith("'")) {
        throw new KsqlException(
            serde + " value is string and should be enclosed between " + "\"'\".");
      }
      serde = serde.substring(1, serde.length() - 1).toUpperCase();
    } else {
      final StructuredDataSource leftSource = analysis.getFromDataSource(0).left;
      serde = leftSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().toString();
    }

    analysis.setIntoFormat(serde);
    analysis.getIntoProperties().put(DdlConfig.VALUE_FORMAT_PROPERTY, serde);

    final Expression avroSchemaFullName =
        sink.getProperties().get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME);

    if ("AVRO".equals(serde)) {
      analysis.getIntoProperties().put(
          DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME,
          avroSchemaFullName != null
              ? avroSchemaFullName : KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME);
    } else if (avroSchemaFullName != null) {
      throw new KsqlException(
          DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME + " is only valid for AVRO topics.");
    }
  }

  private void setIntoTimestampColumnAndFormat(final Sink sink) {
    final Map<String, Expression> properties = sink.getProperties();
    String intoTimestampColumnName = properties
        .get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME).toString().toUpperCase();
    if (!intoTimestampColumnName.startsWith("'") && !intoTimestampColumnName.endsWith("'")) {
      throw new KsqlException(
          intoTimestampColumnName + " value is string and should be enclosed between "
              + "\"'\".");
    }
    intoTimestampColumnName = intoTimestampColumnName.substring(
        1,
        intoTimestampColumnName.length() - 1
    );
    analysis.getIntoProperties().put(
        KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME,
        intoTimestampColumnName
    );

    if (properties.containsKey(DdlConfig.TIMESTAMP_FORMAT_PROPERTY)) {
      final String timestampFormat = StringUtil.cleanQuotes(
          properties.get(DdlConfig.TIMESTAMP_FORMAT_PROPERTY).toString());
      analysis.getIntoProperties().put(DdlConfig.TIMESTAMP_FORMAT_PROPERTY, timestampFormat);
    }
  }

  private static void validateWithClause(final Set<String> withClauseVariables) {

    final Set<String> validSet = new HashSet<>();
    validSet.add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.PARTITION_BY_PROPERTY.toUpperCase());
    validSet.add(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_PARTITIONS.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_REPLICAS.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME.toUpperCase());

    for (final String withVariable : withClauseVariables) {
      if (!validSet.contains(withVariable.toUpperCase())) {
        throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
      }
    }
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private final class Visitor extends DefaultTraversalVisitor<Node, Void> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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

      analyzeExpressions();

      return null;
    }

    private void analyzeExpressions() {
      Schema schema = analysis.getFromDataSources().get(0).getLeft().getSchema();
      boolean isJoinSchema = false;
      if (analysis.getJoin() != null) {
        schema = analysis.getJoin().getSchema();
        isJoinSchema = true;
      }
      final ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(schema, isJoinSchema);

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
      final AliasedRelation left = (AliasedRelation) process(node.getLeft(), context);
      final AliasedRelation right = (AliasedRelation) process(node.getRight(), context);

      final String leftSideName = ((Table) left.getRelation()).getName().getSuffix();
      final StructuredDataSource leftDataSource = metaStore.getSource(leftSideName);
      if (leftDataSource == null) {
        throw new KsqlException(format("Resource %s does not exist.", leftSideName));
      }

      final String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
      final StructuredDataSource rightDataSource = metaStore.getSource(rightSideName);
      if (rightDataSource == null) {
        throw new KsqlException(format("Resource %s does not exist.", rightSideName));
      }

      final String leftAlias = left.getAlias();
      final String rightAlias = right.getAlias();

      final JoinNode.JoinType joinType = getJoinType(node);

      final JoinOn joinOn = (JoinOn) node.getCriteria();
      final ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn
          .getExpression();

      final Pair<String, String> leftSide = fetchKeyFieldName(
          comparisonExpression,
          leftAlias,
          leftDataSource.getSchema()
      );
      final Pair<String, String> rightSide = fetchKeyFieldName(
          comparisonExpression,
          rightAlias,
          rightDataSource.getSchema()
      );

      final String leftKeyFieldName = leftSide.getRight();
      final String rightKeyFieldName = rightSide.getRight();

      if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
        throw new KsqlException("Only equality join criteria is supported.");
      }

      final StructuredDataSourceNode
          leftSourceKafkaTopicNode =
          new StructuredDataSourceNode(
              new PlanNodeId("KafkaTopic_Left"),
              leftDataSource,
              leftAlias
          );
      final StructuredDataSourceNode
          rightSourceKafkaTopicNode =
          new StructuredDataSourceNode(
              new PlanNodeId("KafkaTopic_Right"),
              rightDataSource,
              rightAlias
          );

      final JoinNode joinNode =
          new JoinNode(
              new PlanNodeId("Join"),
              joinType,
              leftSourceKafkaTopicNode,
              rightSourceKafkaTopicNode,
              leftKeyFieldName,
              rightKeyFieldName,
              leftAlias,
              rightAlias,
              node.getWithinExpression().orElse(null),
              leftDataSource.getDataSourceType(),
              rightDataSource.getDataSourceType()
          );

      analysis.setJoin(joinNode);
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

    /**
     * From the join criteria expression fetch the field corresponding to the given source alias.
     */
    private Pair<String, String> fetchKeyFieldName(
        final ComparisonExpression comparisonExpression,
        final String sourceAlias,
        final Schema sourceSchema
    ) {
      Pair<String, String> keyInfo = fetchFieldNameFromExpr(
          comparisonExpression.getLeft(),
          sourceAlias,
          sourceSchema
      );
      if (keyInfo == null) {
        keyInfo = fetchFieldNameFromExpr(
            comparisonExpression.getRight(),
            sourceAlias,
            sourceSchema
        );
      }
      if (keyInfo == null) {
        throw new KsqlException(
            String.format(
                "%s : Invalid join criteria %s. Could not find a join criteria operand for %s. ",
                comparisonExpression.getLocation().map(Objects::toString).orElse(""),
                comparisonExpression, sourceAlias
            )
        );
      }
      return keyInfo;
    }

    /**
     * Given an expression and the source alias detects if the expression type is
     * DereferenceExpression or QualifiedNameReference and if the variable prefix matches the source
     * Alias.
     */
    private Pair<String, String> fetchFieldNameFromExpr(
        final Expression expression, final String sourceAlias,
        final Schema sourceSchema
    ) {
      if (expression instanceof DereferenceExpression) {
        final DereferenceExpression dereferenceExpression =
            (DereferenceExpression) expression;
        final String sourceAliasVal = dereferenceExpression.getBase().toString();
        if (sourceAliasVal.equalsIgnoreCase(sourceAlias)) {
          final String fieldName = dereferenceExpression.getFieldName();
          if (SchemaUtil.getFieldByName(sourceSchema, fieldName).isPresent()) {
            return new Pair<>(sourceAliasVal, fieldName);
          }
        }
      } else if (expression instanceof QualifiedNameReference) {
        final QualifiedNameReference qualifiedNameReference =
            (QualifiedNameReference) expression;
        final String fieldName = qualifiedNameReference.getName().getSuffix();
        if (SchemaUtil.getFieldByName(sourceSchema, fieldName).isPresent()) {
          return new Pair<>(sourceAlias, fieldName);
        }
      }
      return null;
    }


    @Override
    protected Node visitAliasedRelation(final AliasedRelation node, final Void context) {
      final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
      if (metaStore.getSource(structuredDataSourceName) == null) {
        throw new KsqlException(structuredDataSourceName + " does not exist.");
      }

      final Pair<StructuredDataSource, String> fromDataSource =
          new Pair<>(
              metaStore.getSource(structuredDataSourceName),
              node.getAlias()
          );
      analysis.addDataSource(fromDataSource);
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
          // expand * and T.*
          final AllColumns allColumns = (AllColumns) selectItem;
          if ((analysis.getFromDataSources() == null) || (
              analysis.getFromDataSources().isEmpty())) {
            throw new KsqlException("FROM clause was not resolved!");
          }
          if (analysis.getJoin() != null) {
            final JoinNode joinNode = analysis.getJoin();
            for (final Field field : joinNode.getLeft().getSchema().fields()) {
              final QualifiedNameReference qualifiedNameReference =
                  new QualifiedNameReference(allColumns.getLocation(), QualifiedName
                      .of(joinNode.getLeftAlias() + "." + field.name()));
              analysis.addSelectItem(
                  qualifiedNameReference,
                  joinNode.getLeftAlias() + "_" + field.name()
              );
            }
            for (final Field field : joinNode.getRight().getSchema().fields()) {
              final QualifiedNameReference qualifiedNameReference =
                  new QualifiedNameReference(
                      allColumns.getLocation(),
                      QualifiedName.of(joinNode.getRightAlias() + "." + field.name())
                  );
              analysis.addSelectItem(
                  qualifiedNameReference,
                  joinNode.getRightAlias() + "_" + field.name()
              );
            }
          } else {
            for (final Field field : analysis.getFromDataSources().get(0).getLeft().getSchema()
                .fields()) {
              final QualifiedNameReference qualifiedNameReference =
                  new QualifiedNameReference(allColumns.getLocation(), QualifiedName
                      .of(analysis.getFromDataSources().get(0).getRight() + "." + field
                          .name()));
              analysis.addSelectItem(qualifiedNameReference, field.name());
            }
          }
        } else if (selectItem instanceof SingleColumn) {
          final SingleColumn column = (SingleColumn) selectItem;
          analysis.addSelectItem(column.getExpression(), column.getAlias().get());
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
  }
}
