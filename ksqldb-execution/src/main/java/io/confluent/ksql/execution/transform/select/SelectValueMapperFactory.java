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

package io.confluent.ksql.execution.transform.select;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.select.SelectValueMapper.SelectInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Factor class for {@link SelectValueMapper}.
 */
public final class SelectValueMapperFactory {

  private static final String EXP_TYPE = "Select";

  private final CodeGenRunner codeGenerator;

  @VisibleForTesting
  SelectValueMapperFactory(final CodeGenRunner codeGenerator) {
    this.codeGenerator = requireNonNull(codeGenerator, "codeGenerator");
  }

  public static <K> SelectValueMapper<K> create(
      final List<SelectExpression> selectExpressions,
      final LogicalSchema sourceSchema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(sourceSchema, ksqlConfig, functionRegistry);

    return new SelectValueMapperFactory(codeGen).create(selectExpressions);
  }

  @VisibleForTesting
  <K> SelectValueMapper<K> create(
      final List<SelectExpression> selectExpressions
  ) {
    return new SelectValueMapper<>(buildSelects(selectExpressions));
  }

  private List<SelectInfo> buildSelects(final List<SelectExpression> selectExpressions) {
    return selectExpressions.stream()
        .map(this::buildSelect)
        .collect(Collectors.toList());
  }

  private SelectInfo buildSelect(final SelectExpression selectExpression) {
    final ExpressionMetadata evaluator = codeGenerator
        .buildCodeGenFromParseTree(selectExpression.getExpression(), EXP_TYPE);

    return SelectInfo.of(
        selectExpression.getAlias(),
        evaluator
    );
  }
}
