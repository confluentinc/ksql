package io.confluent.kql.analyzer;

import com.google.common.collect.ImmutableList;

import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.metastore.DataSource;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.KQL_STDOUT;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.*;
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

import java.util.List;

public class Analyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  Analysis analysis;
  MetaStore metaStore;

  public Analyzer(Analysis analysis, MetaStore metaStore) {
    this.analysis = analysis;
    this.metaStore = metaStore;
  }

  @Override
  protected Node visitQuerySpecification(QuerySpecification node, AnalysisContext context) {

    process(node.getFrom().get(), new AnalysisContext(null, AnalysisContext.ParentType.FROM));

    process(node.getInto().get(), new AnalysisContext(null, AnalysisContext.ParentType.INTO));
    if (!(analysis.getInto() instanceof KQL_STDOUT)) {
      List<Pair<StructuredDataSource, String>> fromDataSources = analysis.getFromDataSources();

      StructuredDataSource intoStructuredDataSource = (StructuredDataSource) analysis.getInto();
      String intoKafkaTopicName = analysis.getIntoKafkaTopicName();
      if (intoKafkaTopicName == null) {
        intoKafkaTopicName = intoStructuredDataSource.getName();
      }

      KQLTopicSerDe intoTopicSerde = fromDataSources.get(0).getLeft().getKQLTopic()
          .getKqlTopicSerDe();
      if (analysis.getIntoFormat() != null) {
        if (analysis.getIntoFormat().equalsIgnoreCase(DataSource.AVRO_SERDE_NAME)) {
          intoTopicSerde = new KQLAvroTopicSerDe(analysis.getIntoAvroSchemaFilePath(), null);
        } else if (analysis.getIntoFormat().equalsIgnoreCase(DataSource.JSON_SERDE_NAME)) {
          intoTopicSerde = new KQLJsonTopicSerDe();
        } else if (analysis.getIntoFormat().equalsIgnoreCase(DataSource.CSV_SERDE_NAME)) {
          intoTopicSerde = new KQLCsvTopicSerDe();
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

    return null;
  }

  @Override
  protected Node visitJoin(Join node, AnalysisContext context) {
    AliasedRelation left = (AliasedRelation) process(node.getLeft(), context);
    AliasedRelation right = (AliasedRelation) process(node.getRight(), context);

    JoinOn joinOn = (JoinOn) (node.getCriteria().get());
    ComparisonExpression comparisonExpression = (ComparisonExpression) joinOn.getExpression();

    String leftKeyFieldName;
    String rightKeyFieldName;

    if (comparisonExpression.getLeft() instanceof DereferenceExpression) {
      DereferenceExpression
          leftDereferenceExpression =
          (DereferenceExpression) comparisonExpression.getLeft();
      leftKeyFieldName = leftDereferenceExpression.getFieldName();
    } else if (comparisonExpression.getLeft() instanceof QualifiedNameReference) {
      QualifiedNameReference
          leftQualifiedNameReference =
          (QualifiedNameReference) comparisonExpression.getLeft();
      leftKeyFieldName = leftQualifiedNameReference.getName().getSuffix();
    } else {
      throw new KQLException("Join criteria is not supported.");
    }

    if (comparisonExpression.getRight() instanceof DereferenceExpression) {
      DereferenceExpression
          rightDereferenceExpression =
          (DereferenceExpression) comparisonExpression.getRight();
      rightKeyFieldName = rightDereferenceExpression.getFieldName();
    } else if (comparisonExpression.getRight() instanceof QualifiedNameReference) {
      QualifiedNameReference
          rightQualifiedNameReference =
          (QualifiedNameReference) comparisonExpression.getRight();
      rightKeyFieldName = rightQualifiedNameReference.getName().getSuffix();
    } else {
      throw new KQLException("Join criteria is not supported.");
    }

    if (comparisonExpression.getType() != ComparisonExpression.Type.EQUAL) {
      throw new KQLException("Join criteria is not supported.");
    }

    String leftSideName = ((Table) left.getRelation()).getName().getSuffix();
    String leftAlias = left.getAlias();
    String rightSideName = ((Table) right.getRelation()).getName().getSuffix();
    String rightAlias = right.getAlias();

    StructuredDataSource leftDataSource = metaStore.getSource(leftSideName.toUpperCase());
    if (leftDataSource == null) {
      throw new KQLException(leftSideName + " does not exist.");
    }
    StructuredDataSource rightDataSource = metaStore.getSource(rightSideName.toUpperCase());
    if (rightDataSource == null) {
      throw new KQLException(rightSideName + " does not exist.");
    }

    StructuredDataSourceNode
        leftSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Left"), leftDataSource.getSchema(),
                                 leftDataSource.getKeyField(), leftDataSource.getKQLTopic().getTopicName(),
                                 leftAlias.toUpperCase(), leftDataSource.getDataSourceType(),
                                 leftDataSource);
    StructuredDataSourceNode
        rightSourceKafkaTopicNode =
        new StructuredDataSourceNode(new PlanNodeId("KafkaTopic_Right"), rightDataSource.getSchema(),
                                 rightDataSource.getKeyField(), rightDataSource.getKQLTopic().getTopicName(),
                                 rightAlias.toUpperCase(), rightDataSource.getDataSourceType(),
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

  @Override
  protected Node visitAliasedRelation(AliasedRelation node, AnalysisContext context) {
    String structuredDataSourceName = ((Table) node.getRelation()).getName().getSuffix()
        .toUpperCase();
    if (metaStore.getSource(structuredDataSourceName) ==
        null) {
      throw new KQLException(structuredDataSourceName + " does not exist.");
    }
    Pair<StructuredDataSource, String>
        fromDataSource =
        new Pair<>(
            metaStore.getSource(structuredDataSourceName),
            node.getAlias().toUpperCase());
    analysis.getFromDataSources().add(fromDataSource);
    return node;
  }

  @Override
  protected Node visitTable(Table node, AnalysisContext context) {

    StructuredDataSource into;
    if (node.isSTDOut) {
      into = new KQL_STDOUT(KQL_STDOUT.KQL_STDOUT_NAME, null, null, StructuredDataSource.DataSourceType
          .KSTREAM);
    }
    else if (context.getParentType() == AnalysisContext.ParentType.INTO) {
      into = new KQLStream(node.getName().getSuffix(), null, null, null);

      if (node.getProperties().get(DDLConfig.FORMAT_PROPERTY) != null) {
        String serde = node.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString();
        if (!serde.startsWith("'") && !serde.endsWith("'")) {
          throw new KQLException(serde + " value is string and should be enclosed between "
                                 + "\"'\".");
        }
        serde = serde.substring(1,serde.length()-1);
        analysis.setIntoFormat(serde);
        if (serde.equalsIgnoreCase("avro")) {
          String avroSchemaFilePath = "/tmp/"+into.getName()+".avro";
          if (node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) != null) {
            avroSchemaFilePath = node.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
            if (!avroSchemaFilePath.startsWith("'") && !avroSchemaFilePath.endsWith("'")) {
              throw new KQLException(avroSchemaFilePath + " value is string and should be enclosed between "
                                     + "\"'\".");
            }
            avroSchemaFilePath = avroSchemaFilePath.substring(1, avroSchemaFilePath.length()-1);
          }
          analysis.setIntoAvroSchemaFilePath(avroSchemaFilePath);
        }
      }

      if (node.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY) != null) {
        String intoKafkaTopicName = node.getProperties().get(DDLConfig
                                                                    .KAFKA_TOPIC_NAME_PROPERTY).toString();
        if (!intoKafkaTopicName.startsWith("'") && !intoKafkaTopicName.endsWith("'")) {
          throw new KQLException(intoKafkaTopicName + " value is string and should be enclosed between "
                                 + "\"'\".");
        }
        intoKafkaTopicName = intoKafkaTopicName.substring(1,intoKafkaTopicName.length()-1);
        analysis.setIntoKafkaTopicName(intoKafkaTopicName);
      }

//      into =
//          new StructuredDataSourceNode(new PlanNodeId("INTO"), null, null,
//                         node.getName().getSuffix(), node.getName().getSuffix(),
//                                       StructuredDataSource.DataSourceType.KSTREAM, null);

    } else {
      throw new KQLException("INTO clause is not set correctly!");
    }
    analysis.setInto(into);
    return null;
  }

  @Override
  protected Node visitCast(Cast node, AnalysisContext context) {
    return process(node.getExpression(), context);
  }

  @Override
  protected Node visitSelect(Select node, AnalysisContext context) {
    ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

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
            QualifiedNameReference
                qualifiedNameReference =
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
  protected Node visitQualifiedNameReference(QualifiedNameReference node, AnalysisContext context) {
    return visitExpression(node, context);
  }

  private StructuredDataSource analyzeFrom(QuerySpecification node, AnalysisContext context) {

    return null;
  }

  private void analyzeWhere(Node node, AnalysisContext context) {
    analysis.setWhereExpression((Expression) node);
  }

  private void analyzeSelect(Select select, AnalysisContext context) {

  }


}
