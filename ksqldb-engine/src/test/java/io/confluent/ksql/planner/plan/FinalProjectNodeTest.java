/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FinalProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  private static final SourceName SOURCE_NAME = SourceName.of("Bob");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName ALIAS = ColumnName.of("GRACE");

  private static final UnqualifiedColumnReferenceExp COL0_REF =
      new UnqualifiedColumnReferenceExp(COL0);

  private static final UnqualifiedColumnReferenceExp COL1_REF =
      new UnqualifiedColumnReferenceExp(COL1);

  @Mock
  private PlanNode source;
  @Mock
  private FunctionRegistry functionRegistry;

  private List<SelectItem> selects;
  private FinalProjectNode projectNode;

  @Before
  public void setUp() {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);

    selects = ImmutableList.of(new SingleColumn(COL0_REF, Optional.of(ALIAS)));

    projectNode = new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        true
    );
  }

  @Test
  public void shouldValidateKeysByCallingSourceWithProjection() {
    // When:
    projectNode.validateKeyPresent(SOURCE_NAME);

    // Then:
    verify(source).validateKeyPresent(SOURCE_NAME, Projection.of(selects));
  }

  @Test
  public void shouldValidateColumnsByCallingSourceWithProjection() {
    // Given:
    when(source.validateColumns(any())).thenReturn(ImmutableSet.of(COL0_REF));

    // When:
    projectNode.validateColumns(functionRegistry);

    // Then:
    verify(source).validateColumns(RequiredColumns.builder().add(COL0_REF).build());
  }
}