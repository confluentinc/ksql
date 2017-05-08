/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.analyzer;

import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.metastore.DataSource;
import io.confluent.kql.metastore.KQLSTDOUT;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.AliasedRelation;
import io.confluent.kql.parser.tree.AllColumns;
import io.confluent.kql.parser.tree.Cast;
import io.confluent.kql.parser.tree.ComparisonExpression;
import io.confluent.kql.parser.tree.DereferenceExpression;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.GroupBy;
import io.confluent.kql.parser.tree.GroupingElement;
import io.confluent.kql.parser.tree.Join;
import io.confluent.kql.parser.tree.JoinOn;
import io.confluent.kql.parser.tree.Node;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.QualifiedNameReference;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Select;
import io.confluent.kql.parser.tree.SelectItem;
import io.confluent.kql.parser.tree.SingleColumn;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.WindowExpression;
import io.confluent.kql.planner.DefaultTraversalVisitor;
import io.confluent.kql.planner.plan.JoinNode;
import io.confluent.kql.planner.plan.PlanNodeId;
import io.confluent.kql.planner.plan.StructuredDataSourceNode;
import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.Pair;
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
    if (!(analysis.getInto() instanceof KQLSTDOUT)) {
      List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

      StructuredDataSource intoStructuredDataSource = (StructuredDataSource) analysis.getInto();
      String intoKafkaTopicName = analysis.getIntoKafkaTopicName();
      if (intoKafkaTopicName == null) {
        intoKafkaTopicName = intoStructuredDataSource.getName();
      }

      KQLTopicSerDe intoTopicSerde = fromDataSources.get(0).getLeft().getKqlTopic()
          .getKqlTopicSerDe();
      if (analysis.getIntoFormat() != null) {
        switch (analysis.getIntoFormat().toUpperCase()) {
          case DataSource.AVRO_SERDE_NAME:
            intoTopicSerde = new KQLAvroTopicSerDe(analysis.getIntoAvroSchemaFilePath(), null);
            break;
          case DataSource.JSON_SERDE_NAME:
            intoTopicSerde = new KQLJsonTopicSerDe(null);
            break;
          case DataSource.CSV_SERDE_NAME:
            intoTopicSerde = new KQLCsvTopicSerDe();
            break;
        }
      } else {
        if (intoTopicSerde instanceof KQLAvroTopicSerDe) {
          intoTopicSerde = new KQLAvroTopicSerDe(null, null);
        }
      }

      KQLTopic newIntoKQLTopic = new KQLTopic(intoKafkaTopicName,
                                              intoKafkaTopicName, intoTopicSerde);
      KQLStream intoKQLStream = new KQLStream(intoStructuredDataSource.getName(),
                                              null, null, newIntoKQLTopic);
      analysis.setInto(intoKQLStream);
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
      throw new KQLException("Join criteria is not supported.");
    }

    String leftSideName = ((Table) left.getRelation()).getName().getSuffix();
    String leftAlias = left.getAlias();
    String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
    String rightAlias = right.getAlias();

    StructuredDataSource leftDataSource = metaStore.getSource(leftSideName);
    if (leftDataSource == null) {
      throw new KQLException(format("Resource %s does not exist.", leftSideName));
    }
    StructuredDataSource rightDataSource = metaStore.getSource(rightSideName);
    if (rightDataSource == null) {
      throw new KQLException(format("Resource %s does not exist.", rightSideName));
    }

    StructuredDataSourceNode
        leftSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Left"), leftDataSource.getSchema(),
                                     leftDataSource.getKeyField(),
                                     leftDataSource.getKqlTopic().getTopicName(),
                                     leftAlias, leftDataSource.getDataSourceType(),
                                     leftDataSource);
    StructuredDataSourceNode
        rightSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Right"),
                                     rightDataSource.getSchema(),
                                     rightDataSource.getKeyField(),
                                     rightDataSource.getKqlTopic().getTopicName(),
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
        throw new KQLException("Join type is not supported: " + node.getType().name());
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
      throw new KQLException("Join criteria is not supported.");
    }
  }

  @Override
  protected Node visitAliasedRelation(AliasedRelation node, AnalysisContext context) {
    String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix();
    if (metaStore.getSource(structuredDataSourceName) ==
        null) {
      throw new KQLException(structuredDataSourceName + " does not exist.");
    }
    Pair<StructuredDataSource, String>
        fromDataSource =
        new Pair<>(
            metaStore.getSource(structuredDataSourceName),
            node.getAlias());
    analysis.getFromDataSources().add(fromDataSource);
    return node;
  }

  @Override
  protected Node visitTable(final Table node, final AnalysisContext context) {

    StructuredDataSource into;
    if (node.isSTDOut) {
      into =
          new KQLSTDOUT(KQLSTDOUT.KQL_STDOUT_NAME, null, null,
                        StructuredDataSource.DataSourceType.KSTREAM);
    } else if (context.getParentType() == AnalysisContext.ParentType.INTO) {
      into = new KQLStream(node.getName().getSuffix(), null, null, null);

      if (node.getProperties().get(DDLConfig.FORMAT_PROPERTY) != null) {
        String serde = node.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString();
        if (!serde.startsWith("'") && !serde.endsWith("'")) {
          throw new KQLException(
              serde + " value is string and should be enclosed between " + "\"'\".");
        }
        serde = serde.substring(1, serde.length() - 1);
        analysis.setIntoFormat(serde);
        if ("AVRO".equals(serde)) {
          String avroSchemaFilePath = "/tmp/" + into.getName() + ".avro";
          if (node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) != null) {
            avroSchemaFilePath = node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
            if (!avroSchemaFilePath.startsWith("'") && !avroSchemaFilePath.endsWith("'")) {
              throw new KQLException(
                  avroSchemaFilePath + " value is string and should be enclosed between "
                  + "\"'\".");
            }
            avroSchemaFilePath = avroSchemaFilePath.substring(1, avroSchemaFilePath.length() - 1);
          }
          analysis.setIntoAvroSchemaFilePath(avroSchemaFilePath);
        }
      }

      if (node.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
        String
            intoKafkaTopicName =
            node.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
        if (!intoKafkaTopicName.startsWith("'") && !intoKafkaTopicName.endsWith("'")) {
          throw new KQLException(
              intoKafkaTopicName + " value is string and should be enclosed between " + "\"'\".");
        }
        intoKafkaTopicName = intoKafkaTopicName.substring(1, intoKafkaTopicName.length() - 1);
        analysis.setIntoKafkaTopicName(intoKafkaTopicName);
      }
    } else {
      throw new KQLException("INTO clause is not set correctly!");
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
          throw new KQLException("FROM clause was not resolved!");
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
