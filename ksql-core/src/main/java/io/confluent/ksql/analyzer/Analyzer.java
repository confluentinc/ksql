/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.analyzer;

import io.confluent.ksql.ddl.DDLConfig;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KSQLSTDOUT;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTopic;
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
import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.serde.csv.KSQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
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

    process(node.getFrom().get(), new AnalysisContext(null, AnalysisContext.ParentType.FROM));

    process(node.getInto().get(), new AnalysisContext(null, AnalysisContext.ParentType.INTO));
    if (!(analysis.getInto() instanceof KSQLSTDOUT)) {
      List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

      StructuredDataSource intoStructuredDataSource = (StructuredDataSource) analysis.getInto();
      String intoKafkaTopicName = analysis.getIntoKafkaTopicName();
      if (intoKafkaTopicName == null) {
        intoKafkaTopicName = intoStructuredDataSource.getName();
      }

      KSQLTopicSerDe intoTopicSerde = fromDataSources.get(0).getLeft().getKsqlTopic()
          .getKsqlTopicSerDe();
      if (analysis.getIntoFormat() != null) {
        switch (analysis.getIntoFormat().toUpperCase()) {
          case DataSource.AVRO_SERDE_NAME:
            intoTopicSerde = new KSQLAvroTopicSerDe(analysis.getIntoAvroSchemaFilePath(), null);
            break;
          case DataSource.JSON_SERDE_NAME:
            intoTopicSerde = new KSQLJsonTopicSerDe(null);
            break;
          case DataSource.CSV_SERDE_NAME:
            intoTopicSerde = new KSQLCsvTopicSerDe();
            break;
        }
      } else {
        if (intoTopicSerde instanceof KSQLAvroTopicSerDe) {
          intoTopicSerde = new KSQLAvroTopicSerDe(null, null);
        }
      }

      KSQLTopic newIntoKSQLTopic = new KSQLTopic(intoKafkaTopicName,
          intoKafkaTopicName, intoTopicSerde);
      KSQLStream intoKSQLStream = new KSQLStream(intoStructuredDataSource.getName(),
                                              null, null, null, newIntoKSQLTopic);
      analysis.setInto(intoKSQLStream);
    }

    process(node.getSelect(), new AnalysisContext(null, AnalysisContext.ParentType.SELECT));
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
    JoinOn joinOn = (JoinOn) (node.getCriteria().get());
    ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn.getExpression();

    String leftKeyFieldName = fetchKeyFieldName(comparisonExpression.getLeft());
    String rightKeyFieldName = fetchKeyFieldName(comparisonExpression.getRight());

    if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
      throw new KSQLException("Join criteria is not supported.");
    }

    String leftSideName = ((Table) left.getRelation()).getName().getSuffix();
    String leftAlias = left.getAlias();
    String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
    String rightAlias = right.getAlias();

    StructuredDataSource leftDataSource = metaStore.getSource(leftSideName);
    if (leftDataSource == null) {
      throw new KSQLException(format("Resource %s does not exist.", leftSideName));
    }
    if (((Table) left.getRelation()).getProperties() != null) {
      if (((Table) left.getRelation()).getProperties().get(DDLConfig.TIMESTAMP_NAME_PROPERTY) != null) {
        String timestampFieldName = (((Table) left.getRelation())).getProperties().get(DDLConfig
                                                                                           .TIMESTAMP_NAME_PROPERTY).toString().toUpperCase();
        if (!timestampFieldName.startsWith("'") && !timestampFieldName.endsWith("'")) {
          throw new KSQLException("Property name should be String with single qoute.");
        }
        timestampFieldName = timestampFieldName.substring(1, timestampFieldName.length() - 1);
        leftDataSource = leftDataSource.cloneWithTimeField(timestampFieldName);
      }
    }

    StructuredDataSource rightDataSource = metaStore.getSource(rightSideName);
    if (rightDataSource == null) {
      throw new KSQLException(format("Resource %s does not exist.", rightSideName));
    }

    if (((Table) right.getRelation()).getProperties() != null) {
      if (((Table) right.getRelation()).getProperties().get(DDLConfig.TIMESTAMP_NAME_PROPERTY) !=
          null) {
        String timestampFieldName = (((Table) right.getRelation())).getProperties().get(DDLConfig
                                                                                            .TIMESTAMP_NAME_PROPERTY).toString().toUpperCase();
        if (!timestampFieldName.startsWith("'") && !timestampFieldName.endsWith("'")) {
          throw new KSQLException("Property name should be String with single qoute.");
        }
        timestampFieldName = timestampFieldName.substring(1, timestampFieldName.length() - 1);
        rightDataSource = rightDataSource.cloneWithTimeField(timestampFieldName);
      }
    }

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
        throw new KSQLException("Join type is not supported: " + node.getType().name());
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
      throw new KSQLException("Join criteria is not supported.");
    }
  }

  @Override
  protected Node visitAliasedRelation(AliasedRelation node, AnalysisContext context) {
    String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    if (metaStore.getSource(structuredDataSourceName) ==
        null) {
      throw new KSQLException(structuredDataSourceName + " does not exist.");
    }

    StructuredDataSource structuredDataSource = metaStore.getSource(structuredDataSourceName);

    if (((Table) node.getRelation()).getProperties() != null) {
      if (((Table) node.getRelation()).getProperties().get(DDLConfig.TIMESTAMP_NAME_PROPERTY) != null) {
        String timestampFieldName = ((Table) node.getRelation()).getProperties().get(DDLConfig
                                                                                         .TIMESTAMP_NAME_PROPERTY).toString().toUpperCase();
        if (!timestampFieldName.startsWith("'") && !timestampFieldName.endsWith("'")) {
          throw new KSQLException("Property name should be String with single qoute.");
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
    if (node.isSTDOut) {
      into =
          new KSQLSTDOUT(KSQLSTDOUT.KSQL_STDOUT_NAME, null, null, null,
                        StructuredDataSource.DataSourceType.KSTREAM);

    } else if (context.getParentType() == AnalysisContext.ParentType.INTO) {
      into = new KSQLStream(node.getName().getSuffix(), null, null, null, null);

      if (node.getProperties().get(DDLConfig.FORMAT_PROPERTY) != null) {
        String serde = node.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString();
        if (!serde.startsWith("'") && !serde.endsWith("'")) {
          throw new KSQLException(
              serde + " value is string and should be enclosed between " + "\"'\".");
        }
        serde = serde.substring(1, serde.length() - 1);
        analysis.setIntoFormat(serde);
        analysis.getIntoProperties().put(DDLConfig.FORMAT_PROPERTY, serde);
        if ("AVRO".equals(serde)) {
          String avroSchemaFilePath = "/tmp/" + into.getName() + ".avro";
          if (node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) != null) {
            avroSchemaFilePath = node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
            if (!avroSchemaFilePath.startsWith("'") && !avroSchemaFilePath.endsWith("'")) {
              throw new KSQLException(
                  avroSchemaFilePath + " value is string and should be enclosed between "
                      + "\"'\".");
            }
            avroSchemaFilePath = avroSchemaFilePath.substring(1, avroSchemaFilePath.length() - 1);
          }
          analysis.setIntoAvroSchemaFilePath(avroSchemaFilePath);
          analysis.getIntoProperties().put(DDLConfig.AVRO_SCHEMA_FILE, avroSchemaFilePath);

        }
      }

      if (node.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
        String
            intoKafkaTopicName =
            node.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
        if (!intoKafkaTopicName.startsWith("'") && !intoKafkaTopicName.endsWith("'")) {
          throw new KSQLException(
              intoKafkaTopicName + " value is string and should be enclosed between " + "\"'\".");
        }
        intoKafkaTopicName = intoKafkaTopicName.substring(1, intoKafkaTopicName.length() - 1);
        analysis.setIntoKafkaTopicName(intoKafkaTopicName);
        analysis.getIntoProperties().put(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, intoKafkaTopicName);
      }

      if (node.getProperties().get(KSQLConfig.SINK_TIMESTAMP_COLUMN_NAME) != null) {
        String
            intoTimestampColumnName =
            node.getProperties().get(KSQLConfig.SINK_TIMESTAMP_COLUMN_NAME).toString().toUpperCase();
        if (!intoTimestampColumnName.startsWith("'") && !intoTimestampColumnName.endsWith("'")) {
          throw new KSQLException(
              intoTimestampColumnName + " value is string and should be enclosed between " + "\"'\".");
        }
        intoTimestampColumnName = intoTimestampColumnName.substring(1, intoTimestampColumnName.length() - 1);
        analysis.getIntoProperties().put(KSQLConfig.SINK_TIMESTAMP_COLUMN_NAME,
                                         intoTimestampColumnName);
      }

      if (node.getProperties().get(KSQLConfig.SINK_NUMBER_OF_PARTITIONS) != null) {
        try {
          int numberOfPartitions = Integer.parseInt(node.getProperties().get(KSQLConfig.SINK_NUMBER_OF_PARTITIONS).toString());
          analysis.getIntoProperties().put(KSQLConfig.SINK_NUMBER_OF_PARTITIONS, numberOfPartitions);

        } catch (NumberFormatException e) {
          throw new KSQLException("Invalid number of partitions in WITH clause: " + node.getProperties().get(KSQLConfig.SINK_NUMBER_OF_PARTITIONS).toString());
        }
      }

      if (node.getProperties().get(KSQLConfig.SINK_NUMBER_OF_REPLICATIONS) != null) {
        try {
          short numberOfReplications = Short.parseShort(node.getProperties().get(KSQLConfig.SINK_NUMBER_OF_REPLICATIONS).toString());
          analysis.getIntoProperties().put(KSQLConfig.SINK_NUMBER_OF_REPLICATIONS, numberOfReplications);
        } catch (NumberFormatException e) {
          throw new KSQLException("Invalid number of replications in WITH clause: " + node
              .getProperties().get(KSQLConfig.SINK_NUMBER_OF_REPLICATIONS).toString());
        }
      }



    } else {
      throw new KSQLException("INTO clause is not set correctly!");
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
          throw new KSQLException("FROM clause was not resolved!");
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
    for (GroupingElement groupingElement : node.getGroupingElements()) {
//          process(groupingElement, context);
//          analysis.getGroupByExpressions().add(groupingElement.)
    }
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

  private void analyzeWindowExpression(final WindowExpression windowExpression, final AnalysisContext context) {
    analysis.setWindowExpression(windowExpression);
  }

  private void analyzeHaving(final Node node, final AnalysisContext context) {
    analysis.setHavingExpression((Expression) node);
  }
}