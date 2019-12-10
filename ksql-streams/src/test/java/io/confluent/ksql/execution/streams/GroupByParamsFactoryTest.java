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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupByParamsFactoryTest {

  private static final KeyBuilder INT_KEY_BUILDER = StructKeyUtil.keyBuilder(SqlTypes.INTEGER);
  private static final KeyBuilder STRING_KEY_BUILDER = StructKeyUtil.keyBuilder(SqlTypes.STRING);

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("v0"), SqlTypes.DOUBLE)
      .build();

  @Mock
  private ExpressionMetadata groupBy0;

  @Mock
  private ExpressionMetadata groupBy1;

  @Mock
  private GenericRow value;

  private GroupByParams singlePrams;
  private GroupByParams multiParams;

  @Before
  public void setUp() {
    when(groupBy0.getExpressionType()).thenReturn(SqlTypes.INTEGER);

    singlePrams = GroupByParamsFactory.build(SOURCE_SCHEMA, ImmutableList.of(groupBy0));
    multiParams = GroupByParamsFactory.build(SOURCE_SCHEMA, ImmutableList.of(groupBy0, groupBy1));

    when(groupBy0.evaluate(any())).thenReturn(0);
    when(groupBy1.evaluate(any())).thenReturn(0L);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowOnNullParam() {
    new NullPointerTester()
        .setDefault(List.class, ImmutableList.of(groupBy0))
        .setDefault(LogicalSchema.class, SOURCE_SCHEMA)
        .setDefault(SqlType.class, SqlTypes.BIGINT)
        .testStaticMethods(GroupByParamsFactory.class, Visibility.PACKAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptyParam() {
    GroupByParamsFactory.build(SOURCE_SCHEMA, Collections.emptyList());
  }

  @Test
  public void shouldInvokeEvaluatorsWithCorrectParams() {
    // When:
    multiParams.getMapper().apply(value);

    // Then:
    verify(groupBy0).evaluate(value);
    verify(groupBy1).evaluate(value);
  }

  @Test
  public void shouldGenerateSingleExpressionGroupByKey() {
    // Given:
    when(groupBy0.evaluate(any())).thenReturn(10);

    // When:
    final Struct result = singlePrams.getMapper().apply(value);

    // Then:
    assertThat(result, is(INT_KEY_BUILDER.build(10)));
  }

  @Test
  public void shouldGenerateMultiExpressionGroupByKey() {
    // Given:
    when(groupBy0.evaluate(any())).thenReturn(99);
    when(groupBy1.evaluate(any())).thenReturn(-100L);

    // When:
    final Struct result = multiParams.getMapper().apply(value);

    // Then:
    assertThat(result, is(STRING_KEY_BUILDER.build("99|+|-100")));
  }

  @Test
  public void shouldReturnNullIfSingleExpressionResolvesToNull() {
    // Given:
    when(groupBy0.evaluate(any())).thenReturn(null);

    // When:
    final Struct result = singlePrams.getMapper().apply(value);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfAnyMultiExpressionResolvesToNull() {
    // Given:
    when(groupBy0.evaluate(any())).thenReturn(null);

    // When:
    final Struct result = multiParams.getMapper().apply(value);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfExpressionThrowsInSingle() {
    // Given:
    when(groupBy0.evaluate(any())).thenThrow(new RuntimeException("Boom"));

    // When:
    final Struct result = singlePrams.getMapper().apply(value);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullExpressionThrowsInMulti() {
    // Given:
    when(groupBy0.evaluate(any())).thenThrow(new RuntimeException("Boom"));

    // When:
    final Struct result = multiParams.getMapper().apply(value);

    // Then:
    assertThat(result, is(nullValue()));
  }
}
