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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.select.SelectValueMapper.SelectInfo;
import io.confluent.ksql.name.ColumnName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectValueMapperFactoryTest {

  @Mock
  private SelectExpression select_0;
  @Mock
  private SelectExpression select_1;
  @Mock
  private CompiledExpression md_0;
  @Mock
  private CompiledExpression md_1;

  @Before
  public void setUp() {

    when(select_0.getAlias()).thenReturn(ColumnName.of("field_0"));
    when(select_1.getAlias()).thenReturn(ColumnName.of("field_1"));
  }

  @Test
  public void shouldBuildSelects() {
    // When:
    final SelectValueMapper<?> mapper = SelectValueMapperFactory.create(
        ImmutableList.of(select_0, select_1), ImmutableList.of(md_0, md_1));

    // Then:
    assertThat(mapper.getSelects(), contains(
       SelectInfo.of(ColumnName.of("field_0"), md_0),
       SelectInfo.of(ColumnName.of("field_1"), md_1)
    ));
  }
}