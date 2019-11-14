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

package io.confluent.ksql.parser;

import static io.confluent.ksql.parser.tree.JoinMatchers.hasLeft;
import static io.confluent.ksql.parser.tree.JoinMatchers.hasRight;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AstBuilderTest {

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  private static final SourceName TEST1_NAME = SourceName.of("TEST1");
  private static final SourceName TEST2_NAME = SourceName.of("TEST2");
  private static final Table TEST1 = new Table(TEST1_NAME);
  private static final Table TEST2 = new Table(TEST2_NAME);

  private AstBuilder builder;

  @Before
  public void setup() {
    builder = new AstBuilder(META_STORE);
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldExtractUnaliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, TEST1_NAME)));
  }

  @Test
  public void shouldHandleAliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 t;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, SourceName.of("T"))));
  }

  @Test
  public void shouldExtractAsAliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 AS t;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, SourceName.of("T"))));
  }

  @Test
  public void shouldExtractUnaliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 JOIN TEST2"
        + " ON test1.col1 = test2.col1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, TEST1_NAME)));
    assertThat((Join) result.getFrom(), hasRight(new AliasedRelation(TEST2, TEST2_NAME)));
  }

  @Test
  public void shouldHandleAliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 t1 JOIN TEST2 t2"
        + " ON test1.col1 = test2.col1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, SourceName.of("T1"))));
    assertThat((Join) result.getFrom(), hasRight(new AliasedRelation(TEST2, SourceName.of("T2"))));
  }

  @Test
  public void shouldExtractAsAliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 AS t1 JOIN TEST2 AS t2"
        + " ON t1.col1 = t2.col1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, SourceName.of("T1"))));
    assertThat((Join) result.getFrom(), hasRight(new AliasedRelation(TEST2, SourceName.of("T2"))));
  }

  @Test
  public void shouldHandleUnqualifiedSelect() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(column("COL0"), Optional.empty())
    ))));
  }

  @Test
  public void shouldHandleQualifiedSelect() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TEST1.COL0 FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(TEST1_NAME, "COL0"), Optional.empty())
    ))));
  }

  @Test
  public void shouldHandleAliasQualifiedSelect() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT T.COL0 FROM TEST2 T;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column(SourceName.of("T"), "COL0"), Optional.empty())
    ))));
  }

  @Test
  public void shouldThrowOnUnknownSelectQualifier() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT unknown.COL0 FROM TEST1;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'UNKNOWN' is not a valid stream/table name or alias.");

    // When:
    builder.build(stmt);
  }

  @Test
  public void shouldOmitSelectAliasIfNotPresent() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column("COL0"), Optional.empty())
    ))));
  }

  @Test
  public void shouldIncludeSelectAliasIfPresent() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT COL0 AS FOO FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            column("COL0"), Optional.of(ColumnName.of("FOO")))
    ))));
  }

  @Test
  public void shouldHandleUnqualifiedSelectStar() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.empty())))));
  }

  @Test
  public void shouldHandleQualifiedSelectStar() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TEST1.* FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(TEST1_NAME))))));
  }

  @Test
  public void shouldHandleAliasQualifiedSelectStar() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT T.* FROM TEST1 T;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(SourceName.of("T")))))));
  }

  @Test
  public void shouldThrowOnUnknownStarAlias() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT unknown.* FROM TEST1;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'UNKNOWN' is not a valid stream/table name or alias.");

    // When:
    builder.build(stmt);
  }

  @Test
  public void shouldHandleUnqualifiedSelectStarOnJoin() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.empty())))));
  }

  @Test
  public void shouldHandleQualifiedSelectStarOnLeftJoinSource() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT TEST1.* FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(TEST1_NAME))))));
  }

  @Test
  public void shouldHandleQualifiedSelectStarOnRightJoinSource() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT TEST2.* FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(TEST2_NAME))))));
  }

  @Test
  public void shouldHandleAliasQualifiedSelectStarOnLeftJoinSource() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT T1.* FROM TEST1 T1 JOIN TEST2 WITHIN 1 SECOND ON T1.ID = TEST2.ID;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(SourceName.of("T1")))))));
  }

  @Test
  public void shouldHandleAliasQualifiedSelectStarOnRightJoinSource() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT T2.* FROM TEST1 JOIN TEST2 T2 WITHIN 1 SECOND ON TEST1.ID = T2.ID;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(SourceName.of("T2")))))));
  }

  @Test
  public void shouldThrowOnUnknownStarAliasOnJoin() {
    // Given:
    final SingleStatementContext stmt = givenQuery(
        "SELECT unknown.* FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'UNKNOWN' is not a valid stream/table name or alias.");

    // When:
    builder.build(stmt);
  }

  @Test
  public void shouldDefaultToYieldFinalForBareQueries() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat("Should be pull", result.isPullQuery(), is(true));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.FINAL));
  }

  @Test
  public void shouldSupportExplicitEmitChangesOnBareQuery() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1 EMIT CHANGES;");

    // When:
    final Query result = (Query) builder.build(stmt);

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }


  @Test
  public void shouldDefaultToEmitChangesForCsas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X AS SELECT * FROM TEST1;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }

  @Test
  public void shouldDefaultToEmitChangesForCtas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X AS SELECT COUNT(1) FROM TEST1 GROUP BY ROWKEY;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }

  @Test
  public void shouldDefaultToEmitChangesForInsertInto() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("INSERT INTO TEST1 SELECT * FROM TEST2;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitChangesForCsas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X AS SELECT * FROM TEST1 EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitChangesForCtas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X AS SELECT COUNT(1) FROM TEST1 GROUP BY ROWKEY EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }
  @Test
  public void shouldSupportExplicitEmitChangesForInsertInto() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("INSERT INTO TEST1 SELECT * FROM TEST2 EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.build(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getResultMaterialization(), is(ResultMaterialization.CHANGES));
  }

  private static SingleStatementContext givenQuery(final String sql) {
    final List<ParsedStatement> statements = KsqlParserTestUtil.parse(sql);
    assertThat(statements, hasSize(1));
    return statements.get(0).getStatement();
  }

  private static ColumnReferenceExp column(final String fieldName) {
    return new ColumnReferenceExp(ColumnRef.of(Optional.empty(), ColumnName.of(fieldName)));
  }

  private static ColumnReferenceExp column(final SourceName source, final String fieldName) {
    return new ColumnReferenceExp(ColumnRef.of(source, ColumnName.of(fieldName)));
  }
}