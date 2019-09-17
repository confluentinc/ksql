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
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

public class Analysis {

  private Optional<Into> into = Optional.empty();
  private final List<AliasedDataSource> fromDataSources = new ArrayList<>();
  private Optional<JoinInfo> joinInfo = Optional.empty();
  private Expression whereExpression = null;
  private final List<Expression> selectExpressions = new ArrayList<>();
  private final List<String> selectExpressionAlias = new ArrayList<>();
  private final List<Expression> groupByExpressions = new ArrayList<>();
  private WindowExpression windowExpression = null;
  private Optional<String> partitionBy = Optional.empty();
  private ImmutableSet<SerdeOption> serdeOptions = ImmutableSet.of();
  private Expression havingExpression = null;
  private OptionalInt limitClause = OptionalInt.empty();
  private CreateSourceAsProperties withProperties = CreateSourceAsProperties.none();

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

  public List<Expression> getGroupByExpressions() {
    return ImmutableList.copyOf(groupByExpressions);
  }

  void addGroupByExpressions(final Set<Expression> expressions) {
    groupByExpressions.addAll(expressions);
  }

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }

  void setWindowExpression(final WindowExpression windowExpression) {
    this.windowExpression = windowExpression;
  }

  Expression getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = havingExpression;
  }

  public Optional<String> getPartitionBy() {
    return partitionBy;
  }

  void setPartitionBy(final String partitionBy) {
    this.partitionBy = Optional.of(partitionBy);
  }

  public OptionalInt getLimitClause() {
    return limitClause;
  }

  void setLimitClause(final int limitClause) {
    this.limitClause = OptionalInt.of(limitClause);
  }

  void setJoin(final JoinInfo joinInfo) {
    if (fromDataSources.size() <= 1) {
      throw new IllegalStateException("Join info can only be supplied for joins");
    }

    this.joinInfo = Optional.of(joinInfo);
  }

  public Optional<JoinInfo> getJoin() {
    return joinInfo;
  }

  public boolean isJoin() {
    return joinInfo.isPresent();
  }

  public List<AliasedDataSource> getFromDataSources() {
    return ImmutableList.copyOf(fromDataSources);
  }

  public SourceSchemas getFromSourceSchemas() {
    final Map<String, LogicalSchema> schemaBySource = fromDataSources.stream()
        .collect(Collectors.toMap(
            AliasedDataSource::getAlias,
            s -> s.getDataSource().getSchema()
        ));

    return new SourceSchemas(schemaBySource);
  }

  void addDataSource(final String alias, final DataSource<?> dataSource) {
    if (!(dataSource instanceof KsqlStream) && !(dataSource instanceof KsqlTable)) {
      throw new IllegalArgumentException("Data source type not supported yet: " + dataSource);
    }

    fromDataSources.add(new AliasedDataSource(alias, dataSource));
  }

  QualifiedNameReference getDefaultArgument() {
    final String alias = fromDataSources.get(0).getAlias();
    return new QualifiedNameReference(QualifiedName.of(alias, SchemaUtil.ROWTIME_NAME));
  }

  void setSerdeOptions(final Set<SerdeOption> serdeOptions) {
    this.serdeOptions = ImmutableSet.copyOf(serdeOptions);
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  void setProperties(final CreateSourceAsProperties properties) {
    withProperties = Objects.requireNonNull(properties, "properties");
  }

  public CreateSourceAsProperties getProperties() {
    return withProperties;
  }

  @Immutable
  public static final class Into {

    private final String name;
    private final KsqlTopic topic;
    private final boolean create;

    public static <K> Into of(
        final String name,
        final boolean create,
        final KsqlTopic topic
    ) {
      return new Into(name, create, topic);
    }

    private Into(
        final String name,
        final boolean create,
        final KsqlTopic topic
    ) {
      this.name = requireNonNull(name, "name");
      this.create = create;
      this.topic = requireNonNull(topic, "topic");
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
  }

  @Immutable
  public static final class AliasedDataSource {

    private final String alias;
    private final DataSource<?> dataSource;

    AliasedDataSource(
        final String alias,
        final DataSource<?> dataSource
    ) {
      this.alias = Objects.requireNonNull(alias, "alias");
      this.dataSource = Objects.requireNonNull(dataSource, "dataSource");

      if (alias.trim().isEmpty()) {
        throw new IllegalArgumentException("Alias or name can not be empty: '" + alias + "'");
      }
    }

    public String getAlias() {
      return alias;
    }

    public DataSource<?> getDataSource() {
      return dataSource;
    }
  }

  @Immutable
  public static final class JoinInfo {

    private final String leftJoinField;
    private final String rightJoinField;
    private final JoinNode.JoinType type;
    private final Optional<WithinExpression> withinExpression;

    JoinInfo(
        final String leftJoinField,
        final String rightJoinField,
        final JoinType type,
        final Optional<WithinExpression> withinExpression

    ) {
      this.leftJoinField =  Objects.requireNonNull(leftJoinField, "leftJoinField");
      this.rightJoinField =  Objects.requireNonNull(rightJoinField, "rightJoinField");
      this.type = Objects.requireNonNull(type, "type");
      this.withinExpression = Objects.requireNonNull(withinExpression, "withinExpression");

      if (leftJoinField.trim().isEmpty()) {
        throw new IllegalArgumentException("left join field name can not be empty");
      }

      if (rightJoinField.trim().isEmpty()) {
        throw new IllegalArgumentException("right join field name can not be empty");
      }
    }

    public String getLeftJoinField() {
      return leftJoinField;
    }

    public String getRightJoinField() {
      return rightJoinField;
    }

    public JoinType getType() {
      return type;
    }

    public Optional<WithinExpression> getWithinExpression() {
      return withinExpression;
    }
  }
}

