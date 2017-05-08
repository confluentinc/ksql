/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.analyzer;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class Analysis {

  StructuredDataSource into;
  String intoFormat = null;
  // TODO: Maybe have all as properties. At the moment this will only be set if format is avro.
  String intoAvroSchemaFilePath = null;
  String intoKafkaTopicName = null;
  List<Pair<StructuredDataSource, String>> fromDataSources = new ArrayList<>();
  JoinNode join;
  Expression whereExpression = null;
  List<Expression> selectExpressions = new ArrayList<>();
  List<String> selectExpressionAlias = new ArrayList<>();

  List<Expression> groupByExpressions = new ArrayList<>();
  WindowExpression windowExpression = null;

  Expression havingExpression = null;


  public void addSelectItem(final Expression expression, final String alias) {
    selectExpressions.add(expression);
    selectExpressionAlias.add(alias);
  }

  public StructuredDataSource getInto() {
    return into;
  }

  public void setInto(StructuredDataSource into) {
    this.into = into;
  }


  public List<Pair<StructuredDataSource, String>> getFromDataSources() {
    return fromDataSources;
  }

  public void setFromDataSources(List<Pair<StructuredDataSource, String>> fromDataSources) {
    this.fromDataSources = fromDataSources;
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public void setWhereExpression(Expression whereExpression) {
    this.whereExpression = whereExpression;
  }

  public List<Expression> getSelectExpressions() {
    return selectExpressions;
  }

  public void setSelectExpressions(List<Expression> selectExpressions) {
    this.selectExpressions = selectExpressions;
  }

  public List<String> getSelectExpressionAlias() {
    return selectExpressionAlias;
  }

  public void setSelectExpressionAlias(List<String> selectExpressionAlias) {
    this.selectExpressionAlias = selectExpressionAlias;
  }

  public JoinNode getJoin() {
    return join;
  }

  public void setJoin(JoinNode join) {
    this.join = join;
  }

  public void setIntoFormat(String intoFormat) {
    this.intoFormat = intoFormat;
  }

  public void setIntoKafkaTopicName(String intoKafkaTopicName) {
    this.intoKafkaTopicName = intoKafkaTopicName;
  }

  public String getIntoFormat() {
    return intoFormat;
  }

  public String getIntoKafkaTopicName() {
    return intoKafkaTopicName;
  }

  public String getIntoAvroSchemaFilePath() {
    return intoAvroSchemaFilePath;
  }

  public void setIntoAvroSchemaFilePath(String intoAvroSchemaFilePath) {
    this.intoAvroSchemaFilePath = intoAvroSchemaFilePath;
  }

  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
  }

  public void setGroupByExpressions(List<Expression> groupByExpressions) {
    this.groupByExpressions = groupByExpressions;
  }

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }

  public void setWindowExpression(WindowExpression windowExpression) {
    this.windowExpression = windowExpression;
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  public void setHavingExpression(Expression havingExpression) {
    this.havingExpression = havingExpression;
  }
}

