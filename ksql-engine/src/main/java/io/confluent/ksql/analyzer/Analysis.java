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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

public class Analysis {

  private final ResultMaterialization resultMaterialization;
  private Optional<Into> into = Optional.empty();
  private final List<AliasedDataSource> fromDataSources = new ArrayList<>();
  private Optional<JoinInfo> joinInfo = Optional.empty();
  private Optional<Expression> whereExpression = Optional.empty();
  private final List<SelectExpression> selectExpressions = new ArrayList<>();
  private final Set<ColumnRef> selectColumnRefs = new HashSet<>();
  private final List<Expression> groupByExpressions = new ArrayList<>();
  private Optional<WindowExpression> windowExpression = Optional.empty();
  private Optional<ColumnRef> partitionBy = Optional.empty();
  private ImmutableSet<SerdeOption> serdeOptions = ImmutableSet.of();
  private Optional<Expression> havingExpression = Optional.empty();
  private OptionalInt limitClause = OptionalInt.empty();
  private CreateSourceAsProperties withProperties = CreateSourceAsProperties.none();

  public Analysis(final ResultMaterialization resultMaterialization) {
    this.resultMaterialization = requireNonNull(resultMaterialization, "resultMaterialization");
  }

  ResultMaterialization getResultMaterialization() {
    return resultMaterialization;
  }

  void addSelectItem(final Expression expression, final ColumnName alias) {
    selectExpressions.add(SelectExpression.of(alias, expression));
  }

  void addSelectColumnRefs(final Collection<ColumnRef> columnRefs) {
    selectColumnRefs.addAll(columnRefs);
  }

  public Optional<Into> getInto() {
    return into;
  }

  public void setInto(final Into into) {
    this.into = Optional.of(into);
  }

  public Optional<Expression> getWhereExpression() {
    return whereExpression;
  }

  void setWhereExpression(final Expression whereExpression) {
    this.whereExpression = Optional.of(whereExpression);
  }

  public List<SelectExpression> getSelectExpressions() {
    return Collections.unmodifiableList(selectExpressions);
  }

  Set<ColumnRef> getSelectColumnRefs() {
    return Collections.unmodifiableSet(selectColumnRefs);
  }

  public List<Expression> getGroupByExpressions() {
    return ImmutableList.copyOf(groupByExpressions);
  }

  void addGroupByExpressions(final Set<Expression> expressions) {
    groupByExpressions.addAll(expressions);
  }

  public Optional<WindowExpression> getWindowExpression() {
    return windowExpression;
  }

  void setWindowExpression(final WindowExpression windowExpression) {
    this.windowExpression = Optional.of(windowExpression);
  }

  Optional<Expression> getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = Optional.of(havingExpression);
  }

  public Optional<ColumnRef> getPartitionBy() {
    return partitionBy;
  }

  void setPartitionBy(final ColumnRef partitionBy) {
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

  SourceSchemas getFromSourceSchemas() {
    final Map<SourceName, LogicalSchema> schemaBySource = fromDataSources.stream()
        .collect(Collectors.toMap(
            AliasedDataSource::getAlias,
            s -> s.getDataSource().getSchema()
        ));

    return new SourceSchemas(schemaBySource);
  }

  void addDataSource(final SourceName alias, final DataSource<?> dataSource) {
    if (!(dataSource instanceof KsqlStream) && !(dataSource instanceof KsqlTable)) {
      throw new IllegalArgumentException("Data source type not supported yet: " + dataSource);
    }

    fromDataSources.add(new AliasedDataSource(alias, dataSource));
  }

  ColumnReferenceExp getDefaultArgument() {
    final SourceName alias = fromDataSources.get(0).getAlias();
    return new ColumnReferenceExp(ColumnRef.of(alias, SchemaUtil.ROWTIME_NAME));
  }

  void setSerdeOptions(final Set<SerdeOption> serdeOptions) {
    this.serdeOptions = ImmutableSet.copyOf(serdeOptions);
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  void setProperties(final CreateSourceAsProperties properties) {
    withProperties = requireNonNull(properties, "properties");
  }

  public CreateSourceAsProperties getProperties() {
    return withProperties;
  }

  @Immutable
  public static final class Into {

    private final SourceName name;
    private final KsqlTopic topic;
    private final boolean create;

    public static <K> Into of(
        final SourceName name,
        final boolean create,
        final KsqlTopic topic
    ) {
      return new Into(name, create, topic);
    }

    private Into(
        final SourceName name,
        final boolean create,
        final KsqlTopic topic
    ) {
      this.name = requireNonNull(name, "name");
      this.create = create;
      this.topic = requireNonNull(topic, "topic");
    }

    public SourceName getName() {
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

    private final SourceName alias;
    private final DataSource<?> dataSource;

    AliasedDataSource(
        final SourceName alias,
        final DataSource<?> dataSource
    ) {
      this.alias = requireNonNull(alias, "alias");
      this.dataSource = requireNonNull(dataSource, "dataSource");
    }

    public SourceName getAlias() {
      return alias;
    }

    public DataSource<?> getDataSource() {
      return dataSource;
    }
  }

  @Immutable
  public static final class JoinInfo {

    private final ColumnRef leftJoinField;
    private final ColumnRef rightJoinField;
    private final JoinNode.JoinType type;
    private final Optional<WithinExpression> withinExpression;

    JoinInfo(
        final ColumnRef leftJoinField,
        final ColumnRef rightJoinField,
        final JoinType type,
        final Optional<WithinExpression> withinExpression

    ) {
      this.leftJoinField =  requireNonNull(leftJoinField, "leftJoinField");
      this.rightJoinField =  requireNonNull(rightJoinField, "rightJoinField");
      this.type = requireNonNull(type, "type");
      this.withinExpression = requireNonNull(withinExpression, "withinExpression");
    }

    public ColumnRef getLeftJoinField() {
      return leftJoinField;
    }

    public ColumnRef getRightJoinField() {
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

