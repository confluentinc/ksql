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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlStdOut;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
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
import io.confluent.ksql.parser.DefaultTraversalVisitor;
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

import static java.lang.String.format;

public class Analyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  private final String sqlExpression;
  private final Analysis analysis;
  private final MetaStore metaStore;

  public Analyzer(String sqlExpression, Analysis analysis, MetaStore metaStore) {
    this.sqlExpression = sqlExpression;
    this.analysis = analysis;
    this.metaStore = metaStore;
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
      analyzeNonStdOutSink();
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
      String limitStr = node.getLimit().get();
      analysis.setLimitClause(Integer.parseInt(limitStr));
    }
    analyzeExpressions();

    return null;
  }

  private void analyzeNonStdOutSink() {
    List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

    StructuredDataSource intoStructuredDataSource = analysis.getInto();
    String intoKafkaTopicName = analysis.getIntoKafkaTopicName();
    if (intoKafkaTopicName == null) {
      intoKafkaTopicName = intoStructuredDataSource.getName();
    }

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

    KsqlTopic newIntoKsqlTopic = new KsqlTopic(
        intoKafkaTopicName,
        intoKafkaTopicName,
        intoTopicSerde
    );
    KsqlStream intoKsqlStream = new KsqlStream(
        sqlExpression,
        intoStructuredDataSource.getName(),
        null,
        null,
        null,
        newIntoKsqlTopic
    );
    analysis.setInto(intoKsqlStream);
  }

  private void analyzeExpressions() {
    Schema schema = analysis.getFromDataSources().get(0).getLeft().getSchema();
    boolean isJoinSchema = false;
    if (analysis.getJoin() != null) {
      schema = analysis.getJoin().getSchema();
      isJoinSchema = true;
    }
    ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(schema, isJoinSchema);

    for (Expression selectExpression : analysis.getSelectExpressions()) {
      expressionAnalyzer.analyzeExpression(selectExpression);
    }
    if (analysis.getWhereExpression() != null) {
      expressionAnalyzer.analyzeExpression(analysis.getWhereExpression());
    }
    if (!analysis.getGroupByExpressions().isEmpty()) {
      for (Expression expression : analysis.getGroupByExpressions()) {
        expressionAnalyzer.analyzeExpression(expression);
      }
    }
    if (analysis.getHavingExpression() != null) {
      expressionAnalyzer.analyzeExpression(analysis.getHavingExpression());
    }
  }

  @Override
  protected Node visitJoin(final Join node, final AnalysisContext context) {
    AliasedRelation left = (AliasedRelation) process(node.getLeft(), context);
    AliasedRelation right = (AliasedRelation) process(node.getRight(), context);

    String leftSideName = ((Table) left.getRelation()).getName().getSuffix();
    StructuredDataSource leftDataSource = metaStore.getSource(leftSideName);
    if (leftDataSource == null) {
      throw new KsqlException(format("Resource %s does not exist.", leftSideName));
    }

    String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
    StructuredDataSource rightDataSource = metaStore.getSource(rightSideName);
    if (rightDataSource == null) {
      throw new KsqlException(format("Resource %s does not exist.", rightSideName));
    }

    String leftAlias = left.getAlias();
    String rightAlias = right.getAlias();

    JoinNode.Type joinType = getJoinType(node);

    if (!node.getCriteria().isPresent()) {
      throw new KsqlException(String.format(
          "%s Join criteria is not set.",
          node.getLocation().isPresent()
          ? node.getLocation().get().toString()
          : ""
      ));
    }
    JoinOn joinOn = (JoinOn) (node.getCriteria().get());
    ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn.getExpression();

    Pair<String, String> leftSide = fetchKeyFieldName(
        comparisonExpression,
        leftAlias,
        leftDataSource.getSchema()
    );
    Pair<String, String> rightSide = fetchKeyFieldName(
        comparisonExpression,
        rightAlias,
        rightDataSource.getSchema()
    );

    String leftKeyFieldName = leftSide.getRight();
    String rightKeyFieldName = rightSide.getRight();

    if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
      throw new KsqlException("Only equality join criteria is supported.");
    }

    StructuredDataSourceNode
        leftSourceKafkaTopicNode =
        new StructuredDataSourceNode(
            new PlanNodeId("KafkaTopic_Left"),
            leftDataSource,
            leftDataSource.getSchema()
        );
    StructuredDataSourceNode
        rightSourceKafkaTopicNode =
        new StructuredDataSourceNode(
            new PlanNodeId("KafkaTopic_Right"),
            rightDataSource,
            rightDataSource.getSchema()
        );

    JoinNode joinNode =
        new JoinNode(
            new PlanNodeId("Join"),
            joinType,
            leftSourceKafkaTopicNode,
            rightSourceKafkaTopicNode,
            leftKeyFieldName,
            rightKeyFieldName,
            leftAlias,
            rightAlias
        );
    analysis.setJoin(joinNode);
    return null;
  }

  private JoinNode.Type getJoinType(Join node) {
    JoinNode.Type joinType;
    switch (node.getType()) {
      case INNER:
        joinType = JoinNode.Type.INNER;
        break;
      case LEFT:
        joinType = JoinNode.Type.LEFT;
        break;
      case RIGHT:
        joinType = JoinNode.Type.RIGHT;
        break;
      case CROSS:
        joinType = JoinNode.Type.CROSS;
        break;
      case FULL:
        joinType = JoinNode.Type.FULL;
        break;
      default:
        throw new KsqlException("Join type is not supported: " + node.getType().name());
    }
    return joinType;
  }

  /**
   * From the join criteria expression fetch the key field corresponding to the given source
   * alias.
   */
  private Pair<String, String> fetchKeyFieldName(
      ComparisonExpression comparisonExpression,
      String sourceAlias,
      Schema sourceSchema
  ) {
    Pair<String, String> keyInfo = fetchKeyFieldNameFromExpr(
        comparisonExpression.getLeft(),
        sourceAlias,
        sourceSchema
    );
    if (keyInfo == null) {
      keyInfo = fetchKeyFieldNameFromExpr(
          comparisonExpression.getRight(),
          sourceAlias,
          sourceSchema
      );
    }
    if (keyInfo == null) {
      throw new KsqlException(
          String.format(
              "%s : Invalid join criteria %s. Key for %s is not set correctly. ",
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
  private Pair<String, String> fetchKeyFieldNameFromExpr(
      Expression expression, String sourceAlias,
      Schema sourceSchema
  ) {
    if (expression instanceof DereferenceExpression) {
      DereferenceExpression dereferenceExpression =
          (DereferenceExpression) expression;
      String sourceAliasVal = dereferenceExpression.getBase().toString();
      if (sourceAliasVal.equalsIgnoreCase(sourceAlias)) {
        String keyFieldName = dereferenceExpression.getFieldName();
        if (SchemaUtil.getFieldByName(sourceSchema, keyFieldName).isPresent()) {
          return new Pair<>(sourceAliasVal, keyFieldName);
        }
      }
    } else if (expression instanceof QualifiedNameReference) {
      QualifiedNameReference qualifiedNameReference =
          (QualifiedNameReference) expression;
      String keyFieldName = qualifiedNameReference.getName().getSuffix();
      if (SchemaUtil.getFieldByName(sourceSchema, keyFieldName).isPresent()) {
        return new Pair<>(sourceAlias, keyFieldName);
      }
    }
    return null;
  }


  @Override
  protected Node visitAliasedRelation(AliasedRelation node, AnalysisContext context) {
    String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    if (metaStore.getSource(structuredDataSourceName) == null) {
      throw new KsqlException(structuredDataSourceName + " does not exist.");
    }

    Pair<StructuredDataSource, String> fromDataSource =
        new Pair<>(
            metaStore.getSource(structuredDataSourceName).copy(),
            node.getAlias()
        );
    analysis.addDataSource(fromDataSource);
    return node;
  }

  @Override
  protected Node visitTable(final Table node, final AnalysisContext context) {

    StructuredDataSource into;
    if (node.isStdOut) {
      into = new KsqlStdOut(
          KsqlStdOut.KSQL_STDOUT_NAME,
          null,
          null,
          null,
          StructuredDataSource.DataSourceType.KSTREAM
      );
    } else if (context.getParentType() == AnalysisContext.ParentType.INTO) {
      into = analyzeNonStdOutTable(node);
    } else {
      throw new KsqlException("INTO clause is not set correctly!");
    }

    analysis.setInto(into);
    return null;
  }


  @Override
  protected Node visitCast(final Cast node, final AnalysisContext context) {
    return process(node.getExpression(), context);
  }

  @Override
  protected Node visitSelect(final Select node, final AnalysisContext context) {
    for (SelectItem selectItem : node.getSelectItems()) {
      if (selectItem instanceof AllColumns) {
        // expand * and T.*
        AllColumns allColumns = (AllColumns) selectItem;
        if ((this.analysis.getFromDataSources() == null) || (
            this.analysis.getFromDataSources().isEmpty()
          )) {
          throw new KsqlException("FROM clause was not resolved!");
        }
        if (analysis.getJoin() != null) {
          JoinNode joinNode = analysis.getJoin();
          for (Field field : joinNode.getLeft().getSchema().fields()) {
            QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(joinNode.getLeftAlias() + "." + field.name()));
            analysis.addSelectItem(
                qualifiedNameReference,
                joinNode.getLeftAlias() + "_" + field.name()
            );
          }
          for (Field field : joinNode.getRight().getSchema().fields()) {
            QualifiedNameReference qualifiedNameReference =
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
          for (Field field : this.analysis.getFromDataSources().get(0).getLeft().getSchema()
              .fields()) {
            QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(this.analysis.getFromDataSources().get(0).getRight() + "." + field.name()));
            analysis.addSelectItem(qualifiedNameReference, field.name());
          }
        }
      } else if (selectItem instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) selectItem;
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
    for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
      Set<Expression> groupingSet = groupingElement.enumerateGroupingSets().get(0);
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
    StructuredDataSource into = new KsqlStream(
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
      String intoPartitionByColumnName = node.getProperties()
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
        int numberOfPartitions = Integer.parseInt(
            node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS).toString()
        );
        analysis.getIntoProperties().put(
            KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
            numberOfPartitions
        );

      } catch (NumberFormatException e) {
        throw new KsqlException(
            "Invalid number of partitions in WITH clause: "
            + node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS).toString());
      }
    }

    if (node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS) != null) {
      try {
        short numberOfReplications =
            Short.parseShort(
                node.getProperties().get(KsqlConstants.SINK_NUMBER_OF_REPLICAS).toString()
            );
        analysis.getIntoProperties()
            .put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, numberOfReplications);
      } catch (NumberFormatException e) {
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

  private void validateWithClause(Set<String> withClauseVariables) {

    Set<String> validSet = new HashSet<>();
    validSet.add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.PARTITION_BY_PROPERTY.toUpperCase());
    validSet.add(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_PARTITIONS.toUpperCase());
    validSet.add(KsqlConstants.SINK_NUMBER_OF_REPLICAS.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase());

    for (String withVariable : withClauseVariables) {
      if (!validSet.contains(withVariable.toUpperCase())) {
        throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
      }
    }
  }
}
