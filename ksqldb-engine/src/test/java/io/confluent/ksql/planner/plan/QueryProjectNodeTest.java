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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName H = ColumnName.of("H");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName ALIAS = ColumnName.of("GRACE");
  private static final SourceName SOURCE_NAME = SourceName.of("SOURCE");

  private static final UnqualifiedColumnReferenceExp K_REF =
      new UnqualifiedColumnReferenceExp(K);

  private static final UnqualifiedColumnReferenceExp COL0_REF =
      new UnqualifiedColumnReferenceExp(COL0);

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(K, SqlTypes.STRING)
      .headerColumn(H, Optional.empty())
      .build();

  @Mock
  private PlanNode source;
  @Mock
  private MetaStore metaStore;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private RewrittenAnalysis analysis;
  @Mock
  private Analysis.AliasedDataSource aliasedDataSource;
  @Mock
  private DataSource dataSource;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private QueryPlannerOptions plannerOptions;


  private List<SelectItem> selects;

  @Before
  public void setUp() {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(source.resolveSelect(anyInt(), any())).thenAnswer(inv -> inv.getArgument(1));
    when(source.getSchema()).thenReturn(INPUT_SCHEMA);
    when(analysis.getFrom()).thenReturn(aliasedDataSource);
    when(aliasedDataSource.getDataSource()).thenReturn(dataSource);
    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
  }

  @Test
  public void shouldBuildPullQueryIntermediateSchemaSelectKeyNonWindowed() {
    // Given:
    selects = ImmutableList.of(new SingleColumn(K_REF, Optional.of(ALIAS)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(ColumnName.of("K")));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expectedSchema = QueryLogicalPlanUtil.buildIntermediateSchema(
        INPUT_SCHEMA, true, false);
    assertThat(expectedSchema, is(projectNode.getIntermediateSchema()));
  }

  @Test
  public void shouldBuildPullQueryIntermediateSchemaSelectKeyWindowed() {
    // Given:
    selects = ImmutableList.of(new SingleColumn(K_REF, Optional.of(ALIAS)));
    when(keyFormat.isWindowed()).thenReturn(true);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(ColumnName.of("K")));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        true,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expectedSchema = QueryLogicalPlanUtil.buildIntermediateSchema(
        INPUT_SCHEMA, true, true);;
    assertThat(expectedSchema, is(projectNode.getIntermediateSchema()));
  }

  @Test
  public void shouldBuildPullQueryIntermediateSchemaSelectValueNonWindowed() {
    // Given:
    selects = ImmutableList.of(new SingleColumn(COL0_REF, Optional.of(ALIAS)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(ALIAS));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    assertThat(INPUT_SCHEMA.withoutPseudoAndKeyColsInValue(),
        is(projectNode.getIntermediateSchema()));
  }

  @Test
  public void shouldBuildPullQueryOutputSchemaSelectKeyNonWindowed() {
    // Given:
    selects = ImmutableList.of(new SingleColumn(K_REF, Optional.of(K)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(K));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expected = LogicalSchema.builder()
        .keyColumn(K, SqlTypes.STRING)
        .build();

    assertThat(expected, is(projectNode.getSchema()));
  }

  @Test
  public void shouldBuildPullQueryOutputSchemaSelectKeyAndWindowBounds() {
    // Given:
    when(keyFormat.isWindowed()).thenReturn(true);
    when(source.getSchema()).thenReturn(INPUT_SCHEMA.withPseudoAndKeyColsInValue(true));

    final UnqualifiedColumnReferenceExp windowstartRef =
        new UnqualifiedColumnReferenceExp(SystemColumns.WINDOWSTART_NAME);
    final UnqualifiedColumnReferenceExp windowendRef =
        new UnqualifiedColumnReferenceExp(SystemColumns.WINDOWEND_NAME);
    selects = ImmutableList.<SelectItem>builder()
        .add(new SingleColumn(windowstartRef, Optional.of(SystemColumns.WINDOWSTART_NAME)))
        .add((new SingleColumn(windowendRef, Optional.of(SystemColumns.WINDOWEND_NAME))))
        .add((new SingleColumn(K_REF, Optional.of(K)))).build();
    when(analysis.getSelectColumnNames()).thenReturn(
        ImmutableSet.of(SystemColumns.WINDOWSTART_NAME, SystemColumns.WINDOWEND_NAME, K));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        true,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expected = LogicalSchema.builder()
        .keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT)
        .keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT)
        .keyColumn(K, SqlTypes.STRING)
        .build();

    assertThat(expected, is(projectNode.getSchema()));
  }

  @Test
  public void shouldBuildPullQueryOutputSchemaSelectValueAndWindowBounds() {
    // Given:
    when(keyFormat.isWindowed()).thenReturn(true);
    when(source.getSchema()).thenReturn(INPUT_SCHEMA.withPseudoAndKeyColsInValue(true));

    final UnqualifiedColumnReferenceExp windowstartRef =
        new UnqualifiedColumnReferenceExp(SystemColumns.WINDOWSTART_NAME);
    final UnqualifiedColumnReferenceExp windowendRef =
        new UnqualifiedColumnReferenceExp(SystemColumns.WINDOWEND_NAME);
    selects = ImmutableList.<SelectItem>builder()
        .add(new SingleColumn(windowstartRef, Optional.of(SystemColumns.WINDOWSTART_NAME)))
        .add((new SingleColumn(windowendRef, Optional.of(SystemColumns.WINDOWEND_NAME))))
        .add((new SingleColumn(COL0_REF, Optional.of(COL0)))).build();
    when(analysis.getSelectColumnNames()).thenReturn(
        ImmutableSet.of(SystemColumns.WINDOWSTART_NAME, SystemColumns.WINDOWEND_NAME, COL0));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        true,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expected = LogicalSchema.builder()
        .keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT)
        .keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT)
        .valueColumn(COL0, SqlTypes.STRING)
        .build();

    assertThat(expected, is(projectNode.getSchema()));
  }

  @Test
  public void shouldBuildPullQueryOutputSchemaSelectStar() {
    // Given:
    selects = ImmutableList.of(new AllColumns(Optional.of(SOURCE_NAME)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of());

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expectedSchema = INPUT_SCHEMA;
    final LogicalSchema expectedIntermediateSchema = QueryLogicalPlanUtil.buildIntermediateSchema(
        INPUT_SCHEMA, true, false);
    assertThat(expectedIntermediateSchema, is(projectNode.getIntermediateSchema()));
    assertThat(expectedSchema.withoutPseudoAndKeyColsInValue(), is(projectNode.getSchema()));
    assertThrows(
        IllegalStateException.class,
        projectNode::getCompiledSelectExpressions
    );
  }

  @Test
  public void shouldSelectEntireStruct() {
    // Given:
    final ColumnName structColumn = ColumnName.of("STRUCT_COL");

    final LogicalSchema inputSchema = LogicalSchema.builder()
        .keyColumn(K, SqlTypes.STRING)
        .valueColumn(structColumn, SqlTypes.struct()
            .field("NESTED_FIELD1", SqlTypes.STRING)
            .field("NESTED_FIELD2", SqlTypes.INTEGER)
            .build())
        .build();

    when(source.getSchema()).thenReturn(inputSchema);
    selects = ImmutableList.of(new SingleColumn(new UnqualifiedColumnReferenceExp(structColumn), Optional.of(structColumn)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(structColumn));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expectedSchema = LogicalSchema.builder()
        .valueColumn(structColumn, SqlTypes.struct()
            .field("NESTED_FIELD1", SqlTypes.STRING)
            .field("NESTED_FIELD2", SqlTypes.INTEGER)
            .build())
        .build();

    assertThat(expectedSchema, is(projectNode.getSchema()));
  }

  @Test
  public void shouldExpandSelectStructStarColumns() {
    // Given:
    final ColumnName structColumn = ColumnName.of("STRUCT_COL");
    final ColumnName nestedField1 = ColumnName.of("NESTED_FIELD1");
    final ColumnName nestedField2 = ColumnName.of("NESTED_FIELD2");

    final LogicalSchema inputSchema = LogicalSchema.builder()
        .keyColumn(K, SqlTypes.STRING)
        .valueColumn(structColumn, SqlTypes.struct()
            .field("NESTED_FIELD1", SqlTypes.STRING)
            .field("NESTED_FIELD2", SqlTypes.INTEGER)
            .build())
        .build();

    when(source.getSchema()).thenReturn(inputSchema);
    selects = ImmutableList.of(new StructAll(new UnqualifiedColumnReferenceExp(structColumn)));
    when(keyFormat.isWindowed()).thenReturn(false);
    when(analysis.getSelectColumnNames()).thenReturn(ImmutableSet.of(structColumn));

    // When:
    final QueryProjectNode projectNode = new QueryProjectNode(
        NODE_ID,
        source,
        selects,
        metaStore,
        ksqlConfig,
        analysis,
        false,
        plannerOptions,
        false
    );

    // Then:
    final LogicalSchema expectedSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("NESTED_FIELD1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("NESTED_FIELD2"), SqlTypes.INTEGER)
        .build();

    assertThat(expectedSchema, is(projectNode.getSchema()));
  }

}