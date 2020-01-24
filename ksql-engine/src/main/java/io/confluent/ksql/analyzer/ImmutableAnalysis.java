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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

@EffectivelyImmutable
public interface ImmutableAnalysis {

  List<FunctionCall> getTableFunctions();

  List<SelectExpression> getSelectExpressions();

  Optional<Expression> getWhereExpression();

  Optional<Into> getInto();

  Set<ColumnRef> getSelectColumnRefs();

  List<Expression> getGroupByExpressions();

  Optional<WindowExpression> getWindowExpression();

  Optional<Expression> getPartitionBy();

  OptionalInt getLimitClause();

  Optional<JoinInfo> getJoin();

  List<AliasedDataSource> getFromDataSources();

  Set<SerdeOption> getSerdeOptions();

  CreateSourceAsProperties getProperties();

  SourceSchemas getFromSourceSchemas();
}
