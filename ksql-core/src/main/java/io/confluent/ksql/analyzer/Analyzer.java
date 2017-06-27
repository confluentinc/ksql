/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.analyzer;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.DataSource;
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
import io.confluent.ksql.planner.DefaultTraversalVisitor;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.csv.KsqlCsvTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public class Analyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  Analysis analysis;
  MetaStore metaStore;

  public Analyzer(Analysis analysis, MetaStore metaStore) {
    this.analysis = analysis;
    this.metaStore = metaStore;
  }

  @Override
  protected Node visitQuerySpecification(final QuerySpecification node,
                                         final AnalysisContext context) {

    process(node.getFrom().get(),
            new AnalysisContext(null, AnalysisContext.ParentType.FROM));

    process(node.getInto().get(), new AnalysisContext(null,
                                                      AnalysisContext.ParentType.INTO));
    if (!(analysis.getInto() instanceof KsqlStdOut)) {
      analyzeNonStdOutSink();
    }

    process(node.getSelect(), new AnalysisContext(null,
                                                  AnalysisContext.ParentType.SELECT));
    if (node.getWhere().isPresent()) {
      analyzeWhere(node.getWhere().get(), context);
    }
    if (node.getGroupBy().isPresent()) {
      analyzeGroupBy(node.getGroupBy().get(), context);
    }

    if (node.getWindowExpression().isPresent()) {
      analyzeWindowExpression(node.getWindowExpression().get(), context);
    }

    if (node.getHaving().isPresent()) {
      analyzeHaving(node.getHaving().get(), context);
    }

    if (node.getLimit().isPresent()) {
      String limitStr = node.getLimit().get();
      Integer limitInt = Integer.parseInt(limitStr);
      analysis.setLimitClause(Optional.of(limitInt));
    }
    analyzeExpressions();

    return null;
  }

  private void analyzeNonStdOutSink() {
    List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

    StructuredDataSource intoStructuredDataSource = (StructuredDataSource) analysis.getInto();
    String intoKafkaTopicName = analysis.getIntoKafkaTopicName();
    if (intoKafkaTopicName == null) {
      intoKafkaTopicName = intoStructuredDataSource.getName();
    }

    KsqlTopicSerDe intoTopicSerde = fromDataSources.get(0).getLeft().getKsqlTopic()
        .getKsqlTopicSerDe();
    if (analysis.getIntoFormat() != null) {
      switch (analysis.getIntoFormat().toUpperCase()) {
        case DataSource.AVRO_SERDE_NAME:
          intoTopicSerde = new KsqlAvroTopicSerDe(null);
          break;
        case DataSource.JSON_SERDE_NAME:
          intoTopicSerde = new KsqlJsonTopicSerDe(null);
          break;
        case DataSource.CSV_SERDE_NAME:
          intoTopicSerde = new KsqlCsvTopicSerDe();
          break;
        default:
          throw new KsqlException(
              String.format("Unsupported format: %s", analysis.getIntoFormat()));
      }
    } else {
      if (intoTopicSerde instanceof KsqlAvroTopicSerDe) {
        intoTopicSerde = new KsqlAvroTopicSerDe(null);
      }
    }

    KsqlTopic newIntoKsqlTopic = new KsqlTopic(intoKafkaTopicName,
                                               intoKafkaTopicName, intoTopicSerde);
    KsqlStream intoKsqlStream = new KsqlStream(intoStructuredDataSource.getName(),
                                               null, null, null,
                                               newIntoKsqlTopic);
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

    for (Expression selectExpression: analysis.getSelectExpressions()) {
      expressionAnalyzer.analyzeExpression(selectExpression);
    }
    if (analysis.getWhereExpression() != null) {
      expressionAnalyzer.analyzeExpression(analysis.getWhereExpression());
    }
    if (!analysis.getGroupByExpressions().isEmpty()) {
      for (Expression expression: analysis.getGroupByExpressions()) {
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
    leftDataSource = timestampColumn(left, leftDataSource);

    String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
    StructuredDataSource rightDataSource = metaStore.getSource(rightSideName);
    if (rightDataSource == null) {
      throw new KsqlException(format("Resource %s does not exist.", rightSideName));
    }

    rightDataSource = timestampColumn(right, rightDataSource);

    String leftAlias = left.getAlias();
    String rightAlias = right.getAlias();
    StructuredDataSourceNode
        leftSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Left"), leftDataSource.getSchema(),
                                     leftDataSource.getKeyField(),
                                     leftDataSource.getTimestampField(),
                                     leftDataSource.getKsqlTopic().getTopicName(),
                                     leftAlias, leftDataSource.getDataSourceType(),
                                     leftDataSource);
    StructuredDataSourceNode
        rightSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Right"),
                                     rightDataSource.getSchema(),
                                     rightDataSource.getKeyField(),
                                     rightDataSource.getTimestampField(),
                                     rightDataSource.getKsqlTopic().getTopicName(),
                                     rightAlias, rightDataSource.getDataSourceType(),
                                     rightDataSource);

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

    JoinOn joinOn = (JoinOn) (node.getCriteria().get());
    ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn.getExpression();

    String leftKeyFieldName = fetchKeyFieldName(comparisonExpression.getLeft());
    String rightKeyFieldName = fetchKeyFieldName(comparisonExpression.getRight());

    if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
      throw new KsqlException("Join criteria is not supported.");
    }

    JoinNode joinNode =
        new JoinNode(new PlanNodeId("Join"), joinType, leftSourceKafkaTopicNode,
            rightSourceKafkaTopicNode, leftKeyFieldName, rightKeyFieldName, leftAlias,
            rightAlias);
    analysis.setJoin(joinNode);
    return null;
  }

  private String fetchKeyFieldName(Expression expression) {
    if (expression instanceof DereferenceExpression) {
      DereferenceExpression
          leftDereferenceExpression =
          (DereferenceExpression) expression;
      return leftDereferenceExpression.getFieldName();
    } else if (expression instanceof QualifiedNameReference) {
      QualifiedNameReference
          leftQualifiedNameReference =
          (QualifiedNameReference) expression;
      return leftQualifiedNameReference.getName().getSuffix();
    } else {
      throw new KsqlException("Join criteria is not supported.");
    }
  }

  private StructuredDataSource timestampColumn(AliasedRelation aliasedRelation,
                                               StructuredDataSource
                                               structuredDataSource) {
    if (((Table) aliasedRelation.getRelation()).getProperties() != null) {
      if (((Table) aliasedRelation.getRelation()).getProperties()
              .get(DdlConfig.TIMESTAMP_NAME_PROPERTY) != null) {
        String timestampFieldName = (((Table) aliasedRelation.getRelation()))
            .getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY).toString().toUpperCase();
        if (!(timestampFieldName.startsWith("'") && timestampFieldName.endsWith("'"))) {
          throw new KsqlException("Property name should be String with single qoute.");
        }
        timestampFieldName = timestampFieldName.substring(1, timestampFieldName.length() - 1);
        structuredDataSource = structuredDataSource.cloneWithTimeField(timestampFieldName);
      }
    }
    return structuredDataSource;
  }

  @Override
  protected Node visitAliasedRelation(AliasedRelation node, AnalysisContext context) {
    String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    if (metaStore.getSource(structuredDataSourceName)
        == null) {
      throw new KsqlException(structuredDataSourceName + " does not exist.");
    }

    StructuredDataSource structuredDataSource = metaStore.getSource(structuredDataSourceName);

    if (((Table) node.getRelation()).getProperties() != null) {
      if (((Table) node.getRelation()).getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY)
          != null) {
        String timestampFieldName = ((Table) node.getRelation()).getProperties()
            .get(DdlConfig.TIMESTAMP_NAME_PROPERTY).toString().toUpperCase();
        if (!timestampFieldName.startsWith("'") && !timestampFieldName.endsWith("'")) {
          throw new KsqlException("Property name should be String with single qoute.");
        }
        timestampFieldName = timestampFieldName.substring(1, timestampFieldName.length() - 1);
        structuredDataSource = structuredDataSource.cloneWithTimeField(timestampFieldName);
      }
    }

    Pair<StructuredDataSource, String>
        fromDataSource =
        new Pair<>(
            structuredDataSource,
            node.getAlias());
    analysis.getFromDataSources().add(fromDataSource);
    return node;
  }

  @Override
  protected Node visitTable(final Table node, final AnalysisContext context) {

    StructuredDataSource into;
    if (node.isStdOut) {
      into =
          new KsqlStdOut(KsqlStdOut.KSQL_STDOUT_NAME, null, null,
                         null, StructuredDataSource.DataSourceType.KSTREAM);

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
        if ((this.analysis.getFromDataSources() == null) || (this.analysis.getFromDataSources()
            .isEmpty())) {
          throw new KsqlException("FROM clause was not resolved!");
        }
        if (analysis.getJoin() != null) {
          JoinNode joinNode = analysis.getJoin();
          for (Field field : joinNode.getLeft().getSchema().fields()) {
            QualifiedNameReference
                qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(joinNode.getLeftAlias() + "." + field.name()));
            analysis.addSelectItem(qualifiedNameReference,
                joinNode.getLeftAlias() + "_" + field.name());
          }
          for (Field field : joinNode.getRight().getSchema().fields()) {
            QualifiedNameReference qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(joinNode.getRightAlias() + "." + field.name()));
            analysis.addSelectItem(qualifiedNameReference,
                joinNode.getRightAlias() + "_" + field.name());
          }
        } else {
          for (Field field : this.analysis.getFromDataSources().get(0).getLeft().getSchema()
              .fields()) {
            QualifiedNameReference
                qualifiedNameReference =
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
  protected Node visitQualifiedNameReference(final QualifiedNameReference node,
                                             final AnalysisContext context) {
    return visitExpression(node, context);
  }

  @Override
  protected Node visitGroupBy(final GroupBy node, final AnalysisContext context) {
    return null;
  }

  private StructuredDataSource analyzeFrom(final QuerySpecification node,
                                           final AnalysisContext context) {
    return null;
  }

  private void analyzeWhere(final Node node, final AnalysisContext context) {
    analysis.setWhereExpression((Expression) node);
  }

  private void analyzeGroupBy(final GroupBy groupBy, final AnalysisContext context) {
    for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
      Set<Expression> groupingSet = groupingElement.enumerateGroupingSets().get(0);
      analysis.getGroupByExpressions().addAll(groupingSet);
    }
  }

  private void analyzeWindowExpression(final WindowExpression windowExpression,
                                       final AnalysisContext context) {
    analysis.setWindowExpression(windowExpression);
  }

  private void analyzeHaving(final Node node, final AnalysisContext context) {
    analysis.setHavingExpression((Expression) node);
  }

  private StructuredDataSource analyzeNonStdOutTable(final Table node) {
    StructuredDataSource into = new KsqlStream(node.getName().getSuffix(), null,
                                               null, null, null);

    setIntoProperties(into, node);
    return into;
  }

  private void setIntoProperties(final StructuredDataSource into, final Table node) {
    if (node.getProperties().get(DdlConfig.FORMAT_PROPERTY) != null) {
      setIntoTopicFormat(into, node);
    }

    if (node.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
      setIntoTopicName(node);
    }

    if (node.getProperties().get(DdlConfig.PARTITION_BY_PROPERTY) != null) {
      String intoPartitionByColumnName = node.getProperties()
          .get(DdlConfig.PARTITION_BY_PROPERTY).toString().toUpperCase();
      analysis.getIntoProperties().put(DdlConfig.PARTITION_BY_PROPERTY,
                                       intoPartitionByColumnName);
    }

    if (node.getProperties().get(KsqlConfig.SINK_TIMESTAMP_COLUMN_NAME) != null) {
      setIntoTimestampColumn(node);
    }

    if (node.getProperties().get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS) != null) {
      try {
        int numberOfPartitions = Integer.parseInt(node.getProperties()
                                                      .get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS)
                                                      .toString());
        analysis.getIntoProperties().put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS,
                                         numberOfPartitions);

      } catch (NumberFormatException e) {
        throw new KsqlException("Invalid number of partitions in WITH clause: "
                                + node.getProperties().get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS)
                                    .toString());
      }
    }

    if (node.getProperties().get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS) != null) {
      try {
        short numberOfReplications =
            Short.parseShort(node.getProperties().get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS)
                                 .toString());
        analysis.getIntoProperties()
            .put(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS, numberOfReplications);
      } catch (NumberFormatException e) {
        throw new KsqlException("Invalid number of replications in WITH clause: " + node
            .getProperties().get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS).toString());
      }
    }
  }

  private void setIntoTopicName(final Table node) {
    String
        intoKafkaTopicName =
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
    String serde = node.getProperties().get(DdlConfig.FORMAT_PROPERTY).toString();
    if (!serde.startsWith("'") && !serde.endsWith("'")) {
      throw new KsqlException(
          serde + " value is string and should be enclosed between " + "\"'\".");
    }
    serde = serde.substring(1, serde.length() - 1);
    analysis.setIntoFormat(serde);
    analysis.getIntoProperties().put(DdlConfig.FORMAT_PROPERTY, serde);
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
      analysis.setIntoAvroSchemaFilePath(avroSchemaFilePath);
      analysis.getIntoProperties().put(DdlConfig.AVRO_SCHEMA_FILE, avroSchemaFilePath);
    }
  }

  private void setIntoTimestampColumn(final Table node) {
    String
        intoTimestampColumnName = node.getProperties()
        .get(KsqlConfig.SINK_TIMESTAMP_COLUMN_NAME).toString().toUpperCase();
    if (!intoTimestampColumnName.startsWith("'") && !intoTimestampColumnName.endsWith("'")) {
      throw new KsqlException(
          intoTimestampColumnName + " value is string and should be enclosed between "
          + "\"'\".");
    }
    intoTimestampColumnName = intoTimestampColumnName.substring(1,
                                                                intoTimestampColumnName
                                                                    .length() - 1);
    analysis.getIntoProperties().put(KsqlConfig.SINK_TIMESTAMP_COLUMN_NAME,
                                     intoTimestampColumnName);
  }
}