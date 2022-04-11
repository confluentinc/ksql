/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

@EffectivelyImmutable
public interface ImmutableAnalysis {

  List<FunctionCall> getTableFunctions();

  List<FunctionCall> getAggregateFunctions();

  List<SelectItem> getSelectItems();

  Optional<Expression> getWhereExpression();

  Optional<Into> getInto();

  Set<ColumnName> getSelectColumnNames();

  Optional<Expression> getHavingExpression();

  Optional<WindowExpression> getWindowExpression();

  ColumnReferenceExp getDefaultArgument();

  Optional<GroupBy> getGroupBy();

  Optional<RefinementInfo> getRefinementInfo();

  Optional<PartitionBy> getPartitionBy();

  OptionalInt getLimitClause();

  boolean isJoin();

  List<JoinInfo> getJoin();

  List<AliasedDataSource> getAllDataSources();

  CreateSourceAsProperties getProperties();

  SourceSchemas getFromSourceSchemas(boolean postAggregate);

  AliasedDataSource getFrom();

  boolean getOrReplace();
}
