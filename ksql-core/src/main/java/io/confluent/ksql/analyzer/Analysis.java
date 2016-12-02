package io.confluent.ksql.analyzer;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class Analysis {


  DataSource into;
  List<Pair<DataSource, String>> fromDataSources = new ArrayList<>();
  JoinNode join;
  Expression whereExpression = null;
  List<Expression> selectExpressions = new ArrayList<>();
  List<String> selectExpressionAlias = new ArrayList<>();


  public void addSelectItem(Expression expression, String alias) {
    selectExpressions.add(expression);
    selectExpressionAlias.add(alias);
  }

  public DataSource getInto() {
    return into;
  }

  public void setInto(DataSource into) {
    this.into = into;
  }


  public List<Pair<DataSource, String>> getFromDataSources() {
    return fromDataSources;
  }

  public void setFromDataSources(List<Pair<DataSource, String>> fromDataSources) {
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
}
