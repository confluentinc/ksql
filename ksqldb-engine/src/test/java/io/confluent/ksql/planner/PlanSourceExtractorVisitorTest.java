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

package io.confluent.ksql.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlanSourceExtractorVisitorTest {

  private static final SourceName SOURCE_ONE = SourceName.of("ds1");
  private static final SourceName SOURCE_TWO = SourceName.of("ds2");

  @Mock
  private DataSource ds1;
  @Mock
  private DataSource ds2;
  @Mock
  private DataSourceNode dsn1;
  @Mock
  private DataSourceNode dsn2;
  @Mock
  private PlanNode join;
  @Mock
  private PlanNode output;
  private PlanSourceExtractorVisitor extractor;

  @Before
  public void init() {
    when(ds1.getName()).thenReturn(SOURCE_ONE);
    when(ds2.getName()).thenReturn(SOURCE_TWO);

    when(dsn1.getDataSource()).thenReturn(ds1);
    when(dsn2.getDataSource()).thenReturn(ds2);

    when(join.getSources()).thenReturn(ImmutableList.of(dsn1, dsn2));

    when(output.getSources()).thenReturn(ImmutableList.of(join));

    extractor = new PlanSourceExtractorVisitor();
  }

  @Test
  public void shouldExtractSourceNames() {
    assertThat(extractor.extract(dsn1), is(ImmutableSet.of(SOURCE_ONE)));
    assertThat(extractor.extract(dsn2), is(ImmutableSet.of(SOURCE_TWO)));
    assertThat(extractor.extract(join), is(ImmutableSet.of(SOURCE_ONE, SOURCE_TWO)));
    assertThat(extractor.extract(output), is(ImmutableSet.of(SOURCE_ONE, SOURCE_TWO)));
  }
}
