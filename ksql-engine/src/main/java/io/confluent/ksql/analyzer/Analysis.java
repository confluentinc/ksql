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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

public class Analysis {

  private Optional<Into> into = Optional.empty();
  private final List<DataSourceNode> fromDataSources = new ArrayList<>();
  private JoinNode join;
  private Expression whereExpression = null;
  private final List<Expression> selectExpressions = new ArrayList<>();
  private final List<String> selectExpressionAlias = new ArrayList<>();
  private final List<Expression> groupByExpressions = new ArrayList<>();
  private WindowExpression windowExpression = null;
  private Optional<String> timestampColumnName = Optional.empty();
  private Optional<String> timestampFormat = Optional.empty();
  private Optional<String> partitionBy = Optional.empty();
  private ImmutableSet<SerdeOption> serdeOptions = ImmutableSet.of();
  private Expression havingExpression = null;
  private OptionalInt limitClause = OptionalInt.empty();

  void addSelectItem(final Expression expression, final String alias) {
    selectExpressions.add(expression);
    selectExpressionAlias.add(alias);
  }

  public Optional<Into> getInto() {
    return into;
  }

  public void setInto(final Into into) {
    this.into = Optional.of(into);
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  void setWhereExpression(final Expression whereExpression) {
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

  public Optional<String> getTimestampColumnName() {
    return timestampColumnName;
  }

  public void setTimestampColumnName(final String columnName) {
    timestampColumnName = Optional.of(columnName);
  }

  public Optional<String> getTimestampFormat() {
    return timestampFormat;
  }

  public void setTimestampFormat(final String format) {
    timestampFormat = Optional.of(format);
  }

  public Optional<String> getPartitionBy() {
    return partitionBy;
  }

  public void setPartitionBy(final String partitionBy) {
    this.partitionBy = Optional.of(partitionBy);
  }

  public OptionalInt getLimitClause() {
    return limitClause;
  }

  public void setLimitClause(final int limitClause) {
    this.limitClause = OptionalInt.of(limitClause);
  }

  public DataSourceNode getFromDataSource(final int index) {
    return fromDataSources.get(index);
  }

  int getFromDataSourceCount() {
    return fromDataSources.size();
  }

  void addDataSource(final DataSourceNode dataSourceNode) {
    fromDataSources.add(dataSourceNode);
  }

  DereferenceExpression getDefaultArgument() {
    final String base = join == null
        ? fromDataSources.get(0).getAlias()
        : join.getLeftAlias();

    final Expression baseExpression = new QualifiedNameReference(QualifiedName.of(base));
    return new DereferenceExpression(baseExpression, SchemaUtil.ROWTIME_NAME);
  }

  void setSerdeOptions(final Set<SerdeOption> serdeOptions) {
    this.serdeOptions = ImmutableSet.copyOf(serdeOptions);
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  @Immutable
  public static final class Into {

    private final String sqlExpression;
    private final String name;
    private final KsqlTopic topic;
    private final SerdeFactory<?> keySerdeFactory;
    private final boolean create;

    public static <K> Into of(
        final String sqlExpression,
        final String name,
        final boolean create,
        final KsqlTopic topic,
        final SerdeFactory<K> keySerde
    ) {
      return new Into(sqlExpression, name, create, topic, keySerde);
    }

    private Into(
        final String sqlExpression,
        final String name,
        final boolean create,
        final KsqlTopic topic,
        final SerdeFactory<?> keySerdeFactory
    ) {
      this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
      this.name = requireNonNull(name, "name");
      this.create = create;
      this.topic = requireNonNull(topic, "topic");
      this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    }

    public String getSqlExpression() {
      return sqlExpression;
    }

    public String getName() {
      return name;
    }

    public boolean isCreate() {
      return create;
    }

    public KsqlTopic getKsqlTopic() {
      return topic;
    }

    public SerdeFactory<?> getKeySerdeFactory() {
      return keySerdeFactory;
    }
  }
}

