/*
 * Copyright 2022 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.select.SelectValueMapper.SelectInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectionTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GIRAFFE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("MANATEE"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("RACCOON"), SqlTypes.BIGINT)
      .build().withPseudoAndKeyColsInValue(false);

  private static final ColumnName ALIASED_KEY = ColumnName.of("KEY");

  private static final Expression EXPRESSION1 =
      new UnqualifiedColumnReferenceExp(ColumnName.of("GIRAFFE"));

  private static final Expression EXPRESSION2 = new ArithmeticBinaryExpression(
      Operator.ADD,
      new UnqualifiedColumnReferenceExp(ColumnName.of("MANATEE")),
      new UnqualifiedColumnReferenceExp(ColumnName.of("RACCOON"))
  );
  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
      SelectExpression.of(ColumnName.of("FOO"), EXPRESSION1),
      SelectExpression.of(ColumnName.of("BAR"), EXPRESSION2)
  );

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;

  private Selection<String> selection;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    selection = Selection.of(
        SCHEMA,
        ImmutableList.of(ALIASED_KEY),
        Optional.empty(),
        SELECT_EXPRESSIONS,
        ksqlConfig,
        functionRegistry
    );
  }

  @Test
  public void shouldBuildMapperWithCorrectExpressions() {
    // When:
    final SelectValueMapper<String> mapper = selection.getMapper();

    // Then:
    final List<SelectInfo> selectInfos = mapper.getSelects();
    assertThat(
        selectInfos.get(0).getEvaluator().getExpression(),
        equalTo(EXPRESSION1));
    assertThat(
        selectInfos.get(1).getEvaluator().getExpression(),
        equalTo(EXPRESSION2));
  }

  @Test
  public void shouldBuildCorrectResultSchema() {
    // When:
    final LogicalSchema resultSchema = selection.getSchema();

    // Then:
    assertThat(resultSchema, equalTo(LogicalSchema.builder()
        .keyColumn(ALIASED_KEY, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("FOO"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("BAR"), SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldBuildCorrectSchemaWithNoKeyColumnNames() {
    // Given:
    selection = Selection.of(
        SCHEMA,
        ImmutableList.of(),
        Optional.empty(),
        SELECT_EXPRESSIONS,
        ksqlConfig,
        functionRegistry
    );

    // When:
    final LogicalSchema resultSchema = selection.getSchema();

    // Then:
    assertThat(resultSchema, equalTo(LogicalSchema.builder()
        .keyColumn(ColumnName.of("K0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("FOO"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("BAR"), SqlTypes.BIGINT)
        .build()
    ));
  }
}