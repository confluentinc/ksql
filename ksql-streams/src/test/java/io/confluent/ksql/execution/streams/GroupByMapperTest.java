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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.util.StructKeyUtil;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupByMapperTest {

  @Mock
  private ExpressionMetadata groupBy0;

  @Mock
  private ExpressionMetadata groupBy1;

  @Mock
  private Struct key;
  @Mock
  private GenericRow value;

  private GroupByMapper<Struct> mapper;

  @Before
  public void setUp() {
    mapper = new GroupByMapper<>(ImmutableList.of(groupBy0, groupBy1));

    when(groupBy0.evaluate(any())).thenReturn("result0");
    when(groupBy1.evaluate(any())).thenReturn("result1");
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullParam() {
    new GroupByMapper<Struct>(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptyParam() {
    new GroupByMapper<Struct>(Collections.emptyList());
  }

  @Test
  public void shouldInvokeEvaluatorsWithCorrectParams() {
    // When:
    mapper.apply(key, value);

    // Then:
    verify(groupBy0).evaluate(value);
    verify(groupBy1).evaluate(value);
  }

  @Test
  public void shouldGenerateGroupByKey() {
    // When:
    final Struct result = mapper.apply(key, value);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("result0|+|result1")));
  }

  @Test
  public void shouldSupportNullValues() {
    // Given:
    when(groupBy0.evaluate(any())).thenReturn(null);

    // When:
    final Struct result = mapper.apply(key, value);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("null|+|result1")));
  }

  @Test
  public void shouldUseNullIfExpressionThrows() {
    // Given:
    when(groupBy0.evaluate(any())).thenThrow(new RuntimeException("Boom"));

    // When:
    final Struct result = mapper.apply(key, value);

    // Then:
    assertThat(result, is(StructKeyUtil.asStructKey("null|+|result1")));
  }
}
