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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.WindowInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Analysis implements ImmutableAnalysis {

  private final Optional<RefinementInfo> refinementInfo;
  private final Function<Map<SourceName, LogicalSchema>, SourceSchemas>
      sourceSchemasFactory;
  private Optional<Into> into = Optional.empty();
  private final List<AliasedDataSource> allDataSources = new ArrayList<>();
  private final List<JoinInfo> joinInfo = new ArrayList<>();
  private Optional<Expression> whereExpression = Optional.empty();
  private final List<SelectItem> selectItems = new ArrayList<>();
  private final Set<ColumnName> selectColumnNames = new HashSet<>();
  private Optional<GroupBy> groupBy = Optional.empty();
  private Optional<PartitionBy> partitionBy = Optional.empty();
  private Optional<WindowExpression> windowExpression = Optional.empty();
  private Optional<Expression> havingExpression = Optional.empty();
  private OptionalInt limitClause = OptionalInt.empty();
  private CreateSourceAsProperties withProperties = CreateSourceAsProperties.none();
  private final List<FunctionCall> tableFunctions = new ArrayList<>();
  private final List<FunctionCall> aggregateFunctions = new ArrayList<>();
  private boolean orReplace = false;
  private boolean pullLimitClauseEnabled = true;

  public Analysis(
      final Optional<RefinementInfo> refinementInfo,
      final boolean pullLimitClauseEnabled
  ) {
    this(refinementInfo, SourceSchemas::new, pullLimitClauseEnabled);
  }

  @VisibleForTesting
  Analysis(
      final Optional<RefinementInfo> refinementInfo,
      final Function<Map<SourceName, LogicalSchema>, SourceSchemas>
          sourceSchemasFactory,
      final boolean pullLimitClauseEnabled
  ) {
    this.refinementInfo = requireNonNull(refinementInfo, "refinementInfo");
    this.sourceSchemasFactory = requireNonNull(sourceSchemasFactory, "sourceSchemasFactory");
    this.pullLimitClauseEnabled  = pullLimitClauseEnabled;
  }

  void addSelectItem(final SelectItem selectItem) {
    selectItems.add(Objects.requireNonNull(selectItem, "selectItem"));
  }

  void addSelectColumnRefs(final Collection<ColumnName> columnNames) {
    selectColumnNames.addAll(columnNames);
  }

  @Override
  public Optional<Into> getInto() {
    return into;
  }

  public void setInto(final Into into) {
    this.into = Optional.of(into);
  }

  @Override
  public Optional<Expression> getWhereExpression() {
    return whereExpression;
  }

  void setWhereExpression(final Expression whereExpression) {
    this.whereExpression = Optional.of(whereExpression);
  }

  @Override
  public List<SelectItem> getSelectItems() {
    return Collections.unmodifiableList(selectItems);
  }

  @Override
  public Set<ColumnName> getSelectColumnNames() {
    return Collections.unmodifiableSet(selectColumnNames);
  }

  @Override
  public Optional<WindowExpression> getWindowExpression() {
    return windowExpression;
  }

  void setWindowExpression(final WindowExpression windowExpression) {
    this.windowExpression = Optional.of(windowExpression);
  }

  @Override
  public Optional<Expression> getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = Optional.of(havingExpression);
  }

  @Override
  public Optional<GroupBy> getGroupBy() {
    return groupBy;
  }

  @Override
  public Optional<RefinementInfo> getRefinementInfo() {
    return refinementInfo;
  }

  void setGroupBy(final GroupBy groupBy) {
    this.groupBy = Optional.of(groupBy);
  }

  @Override
  public Optional<PartitionBy> getPartitionBy() {
    return partitionBy;
  }

  void setPartitionBy(final PartitionBy partitionBy) {
    this.partitionBy = Optional.of(partitionBy);
  }

  @Override
  public OptionalInt getLimitClause() {
    return limitClause;
  }

  void setLimitClause(final int limitClause) {
    this.limitClause = OptionalInt.of(limitClause);
  }

  void addJoin(final JoinInfo joinInfo) {
    this.joinInfo.add(joinInfo);
  }

  public List<JoinInfo> getJoin() {
    return Collections.unmodifiableList(joinInfo);
  }

  public boolean isJoin() {
    return !joinInfo.isEmpty();
  }

  @Override
  public List<AliasedDataSource> getAllDataSources() {
    return ImmutableList.copyOf(allDataSources);
  }

  @Override
  public SourceSchemas getFromSourceSchemas(final boolean postAggregate) {
    final Map<SourceName, LogicalSchema> schemaBySource = allDataSources.stream()
        .collect(Collectors.toMap(
            AliasedDataSource::getAlias,
            ads -> buildStreamsSchema(ads, postAggregate)
        ));

    return sourceSchemasFactory.apply(schemaBySource);
  }

  Optional<AliasedDataSource> getSourceByAlias(final SourceName name) {
    return allDataSources.stream()
        .filter(source -> source.getAlias().equals(name))
        .findFirst();
  }

  Optional<AliasedDataSource> getSourceByName(final SourceName name) {
    return allDataSources.stream()
        .filter(source -> source.getDataSource().getName().equals(name))
        .findFirst();
  }

  @Override
  public AliasedDataSource getFrom() {
    // we know that the first data source to be visited in the Analyzer is
    // the "FROM" data source
    return allDataSources.get(0);
  }

  @Override
  public boolean getOrReplace() {
    return orReplace;
  }

  public void setOrReplace(final boolean orReplace) {
    this.orReplace = orReplace;
  }

  void addDataSource(final SourceName alias, final DataSource dataSource) {
    if (!(dataSource instanceof KsqlStream) && !(dataSource instanceof KsqlTable)) {
      throw new IllegalArgumentException("Data source type not supported yet: " + dataSource);
    }

    allDataSources.add(new AliasedDataSource(alias, dataSource));
  }

  @Override
  public QualifiedColumnReferenceExp getDefaultArgument() {
    final SourceName alias = allDataSources.get(0).getAlias();
    return new QualifiedColumnReferenceExp(alias, SystemColumns.ROWTIME_NAME);
  }

  void setProperties(final CreateSourceAsProperties properties) {
    withProperties = requireNonNull(properties, "properties");
  }

  @Override
  public CreateSourceAsProperties getProperties() {
    return withProperties;
  }

  void addTableFunction(final FunctionCall functionCall) {
    this.tableFunctions.add(Objects.requireNonNull(functionCall));
  }

  @Override
  public List<FunctionCall> getTableFunctions() {
    return Collections.unmodifiableList(tableFunctions);
  }

  void addAggregateFunction(final FunctionCall functionCall) {
    this.aggregateFunctions.add(Objects.requireNonNull(functionCall));
  }

  @Override
  public List<FunctionCall> getAggregateFunctions() {
    return Collections.unmodifiableList(aggregateFunctions);
  }

  public boolean getPullLimitClauseEnabled() {
    return pullLimitClauseEnabled;
  }

  private LogicalSchema buildStreamsSchema(
      final AliasedDataSource ds,
      final boolean postAggregate
  ) {
    // While the special casing exists to copy WINDOWSTART and WINDOWEND into the value schema
    // for some query types, a matching hack is required here:

    // For windowed sources:
    final boolean windowedSource = ds.getDataSource().getKsqlTopic().getKeyFormat().isWindowed();

    // Post the GROUP BY the window bounds columns are available:
    final boolean windowedGroupBy = postAggregate && windowExpression.isPresent();

    return ds.getDataSource()
        .getSchema()
        .withPseudoAndKeyColsInValue(windowedSource || windowedGroupBy);
  }

  @Immutable
  public static final class Into {

    private final SourceName name;
    private final Optional<KsqlTopic> existingTopic;
    private final Optional<NewTopic> newTopic;

    public static Into existingSink(
        final SourceName name,
        final KsqlTopic topic
    ) {
      return new Into(name, Optional.of(topic), Optional.empty());
    }

    public static Into newSink(
        final SourceName name,
        final String topicName,
        final Optional<WindowInfo> windowInfo,
        final FormatInfo keyFormat,
        final FormatInfo valueFormat
    ) {
      final NewTopic newTopic = new NewTopic(topicName, windowInfo, keyFormat, valueFormat);
      return new Into(name, Optional.empty(), Optional.of(newTopic));
    }

    private Into(
        final SourceName name,
        final Optional<KsqlTopic> existingTopic,
        final Optional<NewTopic> newTopic
    ) {
      this.name = requireNonNull(name, "name");
      this.existingTopic = requireNonNull(existingTopic, "existingTopic");
      this.newTopic = requireNonNull(newTopic, "newTopic");
    }

    public SourceName getName() {
      return name;
    }

    public boolean isCreate() {
      return !existingTopic.isPresent();
    }

    public Optional<KsqlTopic> getExistingTopic() {
      return existingTopic;
    }

    public Optional<NewTopic> getNewTopic() {
      return newTopic;
    }

    @Immutable
    public static class NewTopic {

      private final String topicName;
      private final Optional<WindowInfo> windowInfo;
      private final FormatInfo keyFormat;
      private final FormatInfo valueFormat;

      public NewTopic(
          final String topicName,
          final Optional<WindowInfo> windowInfo,
          final FormatInfo keyFormat,
          final FormatInfo valueFormat
      ) {
        this.topicName = requireNonNull(topicName, "topicName");
        this.windowInfo = requireNonNull(windowInfo, "windowInfo");
        this.keyFormat = requireNonNull(keyFormat, "keyFormat");
        this.valueFormat = requireNonNull(valueFormat, "valueFormat");
      }

      public String getTopicName() {
        return topicName;
      }

      public Optional<WindowInfo> getWindowInfo() {
        return windowInfo;
      }

      public FormatInfo getKeyFormat() {
        return keyFormat;
      }

      public FormatInfo getValueFormat() {
        return valueFormat;
      }
    }
  }

  @Immutable
  public static final class AliasedDataSource {

    private final SourceName alias;
    private final DataSource dataSource;

    public AliasedDataSource(
        final SourceName alias,
        final DataSource dataSource
    ) {
      this.alias = requireNonNull(alias, "alias");
      this.dataSource = requireNonNull(dataSource, "dataSource");
    }

    public SourceName getAlias() {
      return alias;
    }

    public DataSource getDataSource() {
      return dataSource;
    }
  }

  @Immutable
  public static final class JoinInfo {

    private final Expression leftJoinExpression;
    private final Expression rightJoinExpression;
    private final JoinNode.JoinType type;
    private final Optional<WithinExpression> withinExpression;
    private final AliasedDataSource leftSource;
    private final AliasedDataSource rightSource;
    private final boolean flippedJoinCondition;

    JoinInfo(
        final AliasedDataSource leftSource,
        final Expression leftJoinExpression,
        final AliasedDataSource rightSource,
        final Expression rightJoinExpression,
        final JoinType type,
        final boolean flippedJoinCondition,
        final Optional<WithinExpression> withinExpression

    ) {
      this.leftSource = requireNonNull(leftSource, "leftSource");
      this.rightSource = requireNonNull(rightSource, "rightSource");
      this.leftJoinExpression = requireNonNull(leftJoinExpression, "leftJoinExpression");
      this.rightJoinExpression = requireNonNull(rightJoinExpression, "rightJoinExpression");
      this.type = requireNonNull(type, "type");
      this.flippedJoinCondition = flippedJoinCondition;
      this.withinExpression = requireNonNull(withinExpression, "withinExpression");
    }

    public AliasedDataSource getLeftSource() {
      return leftSource;
    }

    public AliasedDataSource getRightSource() {
      return rightSource;
    }

    public Expression getLeftJoinExpression() {
      return leftJoinExpression;
    }

    public Expression getRightJoinExpression() {
      return rightJoinExpression;
    }

    public Expression getFlippedLeftJoinExpression() {
      return flippedJoinCondition ? rightJoinExpression : leftJoinExpression;
    }

    public Expression getFlippedRightJoinExpression() {
      return flippedJoinCondition ? leftJoinExpression : rightJoinExpression;
    }

    public JoinType getType() {
      return type;
    }

    public boolean hasFlippedJoinCondition() {
      return flippedJoinCondition;
    }

    public Optional<WithinExpression> getWithinExpression() {
      return withinExpression;
    }

    public JoinInfo flip() {
      return new JoinInfo(
          rightSource,
          rightJoinExpression,
          leftSource,
          leftJoinExpression,
          type,
          true,
          withinExpression
      );
    }
  }
}

