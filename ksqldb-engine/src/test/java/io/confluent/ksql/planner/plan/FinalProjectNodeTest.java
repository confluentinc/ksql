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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
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
  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName STRUCT_COL1 = ColumnName.of("STRUCT_COL1");
  private static final ColumnName STRUCT_F1 = ColumnName.of("F1");
  private static final ColumnName STRUCT_F2 = ColumnName.of("F2");
  private static final ColumnName ALIAS = ColumnName.of("GRACE");
  private static final ColumnName ALIAS2 = ColumnName.of("PETER");

  private static final UnqualifiedColumnReferenceExp K_REF =
      new UnqualifiedColumnReferenceExp(K);

  private static final UnqualifiedColumnReferenceExp COL0_REF =
      new UnqualifiedColumnReferenceExp(COL0);

  private static final UnqualifiedColumnReferenceExp STRUCT_COL1_REF =
      new UnqualifiedColumnReferenceExp(STRUCT_COL1);

  @Mock
  private PlanNode source;
  @Mock
  private MetaStore metaStore;
  @Mock
  private Analysis.Into into;

  private List<SelectItem> selects;
  private FinalProjectNode projectNode;

  @Before
  public void setUp() {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(source.resolveSelect(anyInt(), any())).thenAnswer(inv -> inv.getArgument(1));
    when(source.getSchema()).thenReturn(LogicalSchema.builder()
        .keyColumn(K, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.STRING)
        .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(K, SqlTypes.STRING)
        .valueColumn(STRUCT_COL1, SqlStruct.builder()
            .field(STRUCT_F1.text(), SqlTypes.STRING)
            .field(STRUCT_F2.text(), SqlStruct.builder()
                .field("F2_1", SqlTypes.INTEGER)
                .field("F2_2", SqlTypes.DOUBLE)
                .build())
            .build())
        .build());

    selects = ImmutableList.of(new SingleColumn(COL0_REF, Optional.of(ALIAS)));

    projectNode = new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        Optional.of(into),
        metaStore
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
  public void shouldNotThrowOnSelectStructStarInProjection() {
    // Given:
    clearInvocations(source);

    // select struct_col1->*
    selects = ImmutableList.of(new StructAll(STRUCT_COL1_REF));

    // When:
    new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        Optional.of(into),
        metaStore
    );

    // Then:
    verify(source).resolveSelect(0, STRUCT_COL1_REF);
  }

  @Test
  public void shouldNotThrowOnSelectStructStarFromNestedStructInProjection() {
    final Expression dereferenceExp = new DereferenceExpression(
        Optional.empty(),
        STRUCT_COL1_REF,
        STRUCT_F2.text()
    );

    // Given:
    clearInvocations(source);

    // select struct_col1->f2->*
    selects = ImmutableList.of(new StructAll(dereferenceExp));

    // When:
    new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        Optional.of(into),
        metaStore
    );

    // Then:
    verify(source).resolveSelect(0, dereferenceExp);
  }

  @Test
  public void shouldNotThrowOnSelectStructReferenceInProjection() {
    // Given:
    clearInvocations(source);

    selects = ImmutableList.of(new SingleColumn(STRUCT_COL1_REF, Optional.of(STRUCT_COL1)));

    // When:
    new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        Optional.of(into),
        metaStore
    );

    // Then:
    verify(source).validateColumns(RequiredColumns.builder().add(STRUCT_COL1_REF).build());
  }

  @Test
  public void shouldNotThrowOnSyntheticKeyColumnInProjection() {
    // Given:
    clearInvocations(source);

    final UnqualifiedColumnReferenceExp syntheticKeyRef =
        new UnqualifiedColumnReferenceExp(ColumnName.of("ROWKEY"));

    selects = ImmutableList.of(new SingleColumn(syntheticKeyRef, Optional.of(ALIAS)));

    // When:
    new FinalProjectNode(
        NODE_ID,
        source,
        selects,
        Optional.of(into),
        metaStore
    );

    // Then:
    verify(source).validateColumns(RequiredColumns.builder().add(syntheticKeyRef).build());
  }

  @Test
  public void shouldThrowOnUnknownSyntheticKeyLikeColumnInProjection() {
    // Given:
    clearInvocations(source);

    final UnqualifiedColumnReferenceExp syntheticKeyRef =
        new UnqualifiedColumnReferenceExp(ColumnName.of("ROWKEY"));

    when(source.validateColumns(any())).thenReturn(ImmutableSet.of(syntheticKeyRef));

    selects = ImmutableList.of(new SingleColumn(syntheticKeyRef, Optional.of(ALIAS)));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new FinalProjectNode(
            NODE_ID,
            source,
            selects,
            Optional.of(into),
            metaStore
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Column 'ROWKEY' cannot be resolved."));
  }

  @Test
  public void shouldThrowOnValidateIfSchemaHasNoValueColumns() {
    // Given:
    selects = ImmutableList.of(new SingleColumn(K_REF, Optional.of(ALIAS)));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new FinalProjectNode(
            NODE_ID,
            source,
            selects,
            Optional.of(into),
            metaStore
        )
    );

    // Then:
    assertThat(e.getMessage(), is("The projection contains no value columns."));
  }

  @Test
  public void shouldThrowOnValidateIfMultipleKeyColumns() {
    // Given:
    selects = ImmutableList.of(
        new SingleColumn(K_REF, Optional.of(ALIAS)),
        new SingleColumn(K_REF, Optional.of(ALIAS2))
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new FinalProjectNode(
            NODE_ID,
            source,
            selects,
            Optional.of(into),
            metaStore
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("The projection contains a key column (`K`) more " +
        "than once, aliased as: GRACE and PETER"));
  }
}