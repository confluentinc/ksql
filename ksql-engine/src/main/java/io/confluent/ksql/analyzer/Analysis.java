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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Analysis {

  private StructuredDataSource into;
  private final Map<String, Object> intoProperties = new HashMap<>();
  private String intoFormat = null;
  private boolean doCreateInto;
  private String intoKafkaTopicName = null;
  private final List<Pair<StructuredDataSource, String>> fromDataSources = new ArrayList<>();
  private JoinNode join;
  private Expression whereExpression = null;
  private final List<Expression> selectExpressions = new ArrayList<>();
  private final List<String> selectExpressionAlias = new ArrayList<>();

  private final List<Expression> groupByExpressions = new ArrayList<>();
  private WindowExpression windowExpression = null;

  private Expression havingExpression = null;

  private Integer limitClause = null;


  void addSelectItem(final Expression expression, final String alias) {
    selectExpressions.add(expression);
    selectExpressionAlias.add(alias);
  }

  public StructuredDataSource getInto() {
    return into;
  }

  public void setInto(final StructuredDataSource into, final boolean doCreateInto) {
    this.into = into;
    this.doCreateInto = doCreateInto;
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
  }


  public List<Pair<StructuredDataSource, String>> getFromDataSources() {
    return fromDataSources;
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public void setWhereExpression(final Expression whereExpression) {
    this.whereExpression = whereExpression;
  }

  public List<Expression> getSelectExpressions() {
    return selectExpressions;
  }

  public List<String> getSelectExpressionAlias() {
    return selectExpressionAlias;
  }

  public JoinNode getJoin() {
    return join;
  }

  public void setJoin(final JoinNode join) {
    this.join = join;
  }

  public void setIntoFormat(final String intoFormat) {
    this.intoFormat = intoFormat;
  }

  public void setIntoKafkaTopicName(final String intoKafkaTopicName) {
    this.intoKafkaTopicName = intoKafkaTopicName;
  }

  public String getIntoFormat() {
    return intoFormat;
  }

  public String getIntoKafkaTopicName() {
    return intoKafkaTopicName;
  }

  public List<Expression> getGroupByExpressions() {
    return ImmutableList.copyOf(groupByExpressions);
  }

  void addGroupByExpressions(final Set<Expression> expressions) {
    groupByExpressions.addAll(expressions);
  }

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }

  public void setWindowExpression(final WindowExpression windowExpression) {
    this.windowExpression = windowExpression;
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  public void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = havingExpression;
  }

  public Map<String, Object> getIntoProperties() {
    return intoProperties;
  }

  public Optional<Integer> getLimitClause() {
    return Optional.ofNullable(limitClause);
  }

  public void setLimitClause(final Integer limitClause) {
    this.limitClause = limitClause;
  }

  public Pair<StructuredDataSource, String> getFromDataSource(final int index) {
    return fromDataSources.get(index);
  }

  void addDataSource(final Pair<StructuredDataSource, String> fromDataSource) {
    fromDataSources.add(fromDataSource);
  }

  public DereferenceExpression getDefaultArgument() {
    final String base = join == null
        ? fromDataSources.get(0).getRight()
        : join.getLeftAlias();

    final Expression baseExpression = new QualifiedNameReference(QualifiedName.of(base));
    return new DereferenceExpression(baseExpression, SchemaUtil.ROWTIME_NAME);
  }
}

