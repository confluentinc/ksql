/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.analyzer;

import static java.lang.String.format;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlStdOut;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
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
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class Analyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

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
  public Analyzer(final String sqlExpression,
                  final Analysis analysis,
                  final MetaStore metaStore,
                  final String topicPrefix) {
    this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.topicPrefix = Objects.requireNonNull(topicPrefix, "topicPrefix");
  }

  @Override
  protected Node visitQuerySpecification(
      final QuerySpecification node,
      final AnalysisContext context
  ) {

    process(
        node.getFrom(),
        new AnalysisContext(AnalysisContext.ParentType.FROM)
    );

    process(node.getInto(), new AnalysisContext(
        AnalysisContext.ParentType.INTO));
    if (!(analysis.getInto() instanceof KsqlStdOut)) {
      analyzeNonStdOutSink(node.isShouldCreateInto());
    }

    process(node.getSelect(), new AnalysisContext(
        AnalysisContext.ParentType.SELECT));
    if (node.getWhere().isPresent()) {
      analyzeWhere(node.getWhere().get());
    }
    if (node.getGroupBy().isPresent()) {
      analyzeGroupBy(node.getGroupBy().get());
    }

    if (node.getWindowExpression().isPresent()) {
      analyzeWindowExpression(node.getWindowExpression().get());
    }

    if (node.getHaving().isPresent()) {
      analyzeHaving(node.getHaving().get());
    }

    if (node.getLimit().isPresent()) {
      final String limitStr = node.getLimit().get();
      analysis.setLimitClause(Integer.parseInt(limitStr));
    }
    analyzeExpressions();

    return null;
  }

  private void analyzeNonStdOutSink(final boolean doCreateInto) {
    final List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

    final StructuredDataSource intoStructuredDataSource = analysis.getInto();
    final String intoKafkaTopicName = analysis.getIntoKafkaTopicName() == null
                                      ? topicPrefix + intoStructuredDataSource.getName()
                                      : analysis.getIntoKafkaTopicName();

    final KsqlTopic newIntoKsqlTopic;
    if (doCreateInto) {
      KsqlTopicSerDe intoTopicSerde = fromDataSources.get(0).getLeft().getKsqlTopic()
          .getKsqlTopicSerDe();
      if (analysis.getIntoFormat() != null) {
        switch (analysis.getIntoFormat().toUpperCase()) {
          case DataSource.AVRO_SERDE_NAME:
            intoTopicSerde = new KsqlAvroTopicSerDe();
            break;
          case DataSource.JSON_SERDE_NAME:
            intoTopicSerde = new KsqlJsonTopicSerDe();
            break;
          case DataSource.DELIMITED_SERDE_NAME:
            intoTopicSerde = new KsqlDelimitedTopicSerDe();
            break;
          default:
            throw new KsqlException(
                String.format("Unsupported format: %s", analysis.getIntoFormat()));
        }
      } else {
        if (intoTopicSerde instanceof KsqlAvroTopicSerDe) {
          intoTopicSerde = new KsqlAvroTopicSerDe();
        }
      }

      newIntoKsqlTopic = new KsqlTopic(
          intoStructuredDataSource.getName(),
          intoKafkaTopicName,
          intoTopicSerde
      );
    } else {
      newIntoKsqlTopic = metaStore.getTopic(intoStructuredDataSource.getName());
      if (newIntoKsqlTopic == null) {
        throw new KsqlException(
            "Sink topic " + intoKafkaTopicName + " does not exist in the metastore.");
      }
    }

    final KsqlStream intoKsqlStream = new KsqlStream(
        sqlExpression,
        intoStructuredDataSource.getName(),
        null,
        null,
        null,
        newIntoKsqlTopic
    );
    analysis.setInto(intoKsqlStream, doCreateInto);

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
    if (!analysis.getGroupByExpressions().isEmpty()) {
      for (final Expression expression : analysis.getGroupByExpressions()) {
        expressionAnalyzer.analyzeExpression(expression);
      }
    }
    if (analysis.getHavingExpression() != null) {
      expressionAnalyzer.analyzeExpression(analysis.getHavingExpression());
    }
  }

  @Override
  protected Node visitJoin(final Join node, final AnalysisContext context) {
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

    if (!node.getCriteria().isPresent()) {
      throw new KsqlException(String.format(
          "%s Join criteria is not set.",
          node.getLocation().isPresent()
          ? node.getLocation().get().toString()
          : ""
      ));
    }
    final JoinOn joinOn = (JoinOn) (node.getCriteria().get());
    final ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn.getExpression();

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
            leftDataSource.getSchema()
        );
    final StructuredDataSourceNode
        rightSourceKafkaTopicNode =
        new StructuredDataSourceNode(
            new PlanNodeId("KafkaTopic_Right"),
            rightDataSource,
            rightDataSource.getSchema()
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
   * From the join criteria expression fetch the field corresponding to the given source
   * alias.
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
              comparisonExpression.getLocation().isPresent()
              ? comparisonExpression.getLocation().get().toString()
              : "", comparisonExpression, sourceAlias
          )
      );
    }
    return keyInfo;
  }

  /**
   * Given an expression and the source alias detects if the expression type is
   * DereferenceExpression
   * or QualifiedNameReference and if the variable prefix matches the source Alias.
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
  protected Node visitAliasedRelation(final AliasedRelation node, final AnalysisContext context) {
    final String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    if (metaStore.getSource(structuredDataSourceName) == null) {
      throw new KsqlException(structuredDataSourceName + " does not exist.");
    }

    final Pair<StructuredDataSource, String> fromDataSource =
        new Pair<>(
            metaStore.getSource(structuredDataSourceName).copy(),
            node.getAlias()
        );
    analysis.addDataSource(fromDataSource);
    return node;
  }

  @Override
  protected Node visitTable(final Table node, final AnalysisContext context) {

    final StructuredDataSource into;
    if (node.isStdOut()) {
      into = new KsqlStdOut(
          KsqlStdOut.KSQL_STDOUT_NAME,
          null,
          null,
          null,
          StructuredDataSource.DataSourceType.KSTREAM);
      analysis.setInto(into, false);
    } else if (context.getParentType() == AnalysisContext.ParentType.INTO) {
      into = analyzeNonStdOutTable(node);
      analysis.setInto(into, true);
    } else {
      throw new KsqlException("INTO clause is not set correctly!");
    }
    return null;
  }


  @Override
  protected Node visitCast(final Cast node, final AnalysisContext context) {
    return process(node.getExpression(), context);
  }

  @Override
  protected Node visitSelect(final Select node, final AnalysisContext context) {
    for (final SelectItem selectItem : node.getSelectItems()) {
      if (selectItem instanceof AllColumns) {
        // expand * and T.*
        final AllColumns allColumns = (AllColumns) selectItem;
        if ((this.analysis.getFromDataSources() == null) || (
            this.analysis.getFromDataSources().isEmpty()
          )) {
          throw new KsqlException("FROM clause was not resolved!");
        }
        if (analysis.getJoin() != null) {
          final JoinNode joinNode = analysis.getJoin();
          for (final Field field : joinNode.getLeft().getSchema().fields()) {
            final QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(joinNode.getLeftAlias() + "." + field.name()));
            analysis.addSelectItem(
                qualifiedNameReference,
                joinNode.getLeftAlias() + "_" + field.name()
            );
          }
          for (final Field field : joinNode.getRight().getSchema().fields()) {
            final QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(
                    allColumns.getLocation().get(),
                    QualifiedName.of(joinNode.getRightAlias() + "." + field.name())
                );
            analysis.addSelectItem(
                qualifiedNameReference,
                joinNode.getRightAlias() + "_" + field.name()
            );
          }
        } else {
          for (final Field field : this.analysis.getFromDataSources().get(0).getLeft().getSchema()
              .fields()) {
            final QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(this.analysis.getFromDataSources().get(0).getRight() + "." + field.name()));
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
      final AnalysisContext context
  ) {
    return visitExpression(node, context);
  }

  @Override
  protected Node visitGroupBy(final GroupBy node, final AnalysisContext context) {
    return null;
  }

  private void analyzeWhere(final Node node) {
    analysis.setWhereExpression((Expression) node);
  }

  private void analyzeGroupBy(final GroupBy groupBy) {
    for (final GroupingElement groupingElement : groupBy.getGroupingElements()) {
      final Set<Expression> groupingSet = groupingElement.enumerateGroupingSets().get(0);
      analysis.getGroupByExpressions().addAll(groupingSet);
    }
  }

  private void analyzeWindowExpression(final WindowExpression windowExpression) {
    analysis.setWindowExpression(windowExpression);
  }

  private void analyzeHaving(final Node node) {
    analysis.setHavingExpression((Expression) node);
  }

  private StructuredDataSource analyzeNonStdOutTable(final Table node) {
    final StructuredDataSource into = new KsqlStream(
        sqlExpression,
        node.getName().getSuffix(),
        null,
        null,
        null,
        null
    );

    setIntoProperties(into, node);
    return into;
  }

  private void setIntoProperties(final StructuredDataSource into, final Table node) {

    validateWithClause(node.getProperties().keySet());

    if (node.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY) != null) {
      setIntoTopicFormat(into, node);
    }

    if (node.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
      setIntoTopicName(node);
    }

    if (node.getProperties().get(DdlConfig.PARTITION_BY_PROPERTY) != null) {
      final String intoPartitionByColumnName = node.getProperties()
          .get(DdlConfig.PARTITION_BY_PROPERTY)
          .toString()
          .toUpperCase();
      analysis.getIntoProperties().put(
          DdlConfig.PARTITION_BY_PROPERTY,
          intoPartitionByColumnName
      );
    }

    if (node.getProperties().get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME) != null) {
      setIntoTimestampColumnAndFormat(node);
    }

    if (node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS) != null) {
      try {
        final int numberOfPartitions = Integer.parseInt(
            node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS).toString()
        );
        analysis.getIntoProperties().put(
            KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
            numberOfPartitions
        );

      } catch (final NumberFormatException e) {
        throw new KsqlException(
            "Invalid number of partitions in WITH clause: "
            + node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS).toString());
      }
    }

    if (node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS) != null) {
      try {
        final short numberOfReplications =
            Short.parseShort(
                node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS).toString()
            );
        analysis.getIntoProperties()
            .put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, numberOfReplications);
      } catch (final NumberFormatException e) {
        throw new KsqlException("Invalid number of replications in WITH clause: " + node
            .getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS).toString());
      }
    }
  }

  private void setIntoTopicName(final Table node) {
    String intoKafkaTopicName =
        node.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    if (!intoKafkaTopicName.startsWith("'") && !intoKafkaTopicName.endsWith("'")) {
      throw new KsqlException(
          intoKafkaTopicName + " value is string and should be enclosed between " + "\"'\".");
    }
    intoKafkaTopicName = intoKafkaTopicName.substring(1, intoKafkaTopicName.length() - 1);
    analysis.setIntoKafkaTopicName(intoKafkaTopicName);
    analysis.getIntoProperties().put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, intoKafkaTopicName);
  }

  private void setIntoTopicFormat(final StructuredDataSource into, final Table node) {
    String serde = node.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString();
    if (!serde.startsWith("'") && !serde.endsWith("'")) {
      throw new KsqlException(
          serde + " value is string and should be enclosed between " + "\"'\".");
    }
    serde = serde.substring(1, serde.length() - 1);
    analysis.setIntoFormat(serde);
    analysis.getIntoProperties().put(DdlConfig.VALUE_FORMAT_PROPERTY, serde);
    if ("AVRO".equals(serde)) {
      String avroSchemaFilePath = "/tmp/" + into.getName() + ".avro";
      if (node.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE) != null) {
        avroSchemaFilePath = node.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE).toString();
        if (!avroSchemaFilePath.startsWith("'") && !avroSchemaFilePath.endsWith("'")) {
          throw new KsqlException(
              avroSchemaFilePath + " value is string and should be enclosed between "
              + "\"'\".");
        }
        avroSchemaFilePath = avroSchemaFilePath.substring(1, avroSchemaFilePath.length() - 1);
      }
      analysis.getIntoProperties().put(DdlConfig.AVRO_SCHEMA_FILE, avroSchemaFilePath);
    }
  }

  private void setIntoTimestampColumnAndFormat(final Table node) {
    final Map<String, Expression> properties = node.getProperties();
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

  private void validateWithClause(final Set<String> withClauseVariables) {

    final Set<String> validSet = new HashSet<>();
    validSet.add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.PARTITION_BY_PROPERTY.toUpperCase());
    validSet.add(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_PARTITIONS.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_REPLICAS.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase());

    for (final String withVariable : withClauseVariables) {
      if (!validSet.contains(withVariable.toUpperCase())) {
        throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
      }
    }
  }
}
