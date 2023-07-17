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
import static io.confluent.ksql.parser.tree.JoinMatchers.hasRights;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class AstBuilderTest {

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  private static final SourceName TEST1_NAME = SourceName.of("TEST1");
  private static final SourceName TEST2_NAME = SourceName.of("TEST2");
  private static final SourceName TEST3_NAME = SourceName.of("TEST3");
  private static final Table TEST1 = new Table(TEST1_NAME);
  private static final Table TEST2 = new Table(TEST2_NAME);
  private static final Table TEST3 = new Table(TEST3_NAME);

  private AstBuilder builder;

  @Before
  public void setup() {
    builder = new AstBuilder(META_STORE);
  }

  @Test
  public void shouldExtractUnaliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, TEST1_NAME)));
  }

  @Test
  public void shouldHandleAliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 t;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, SourceName.of("T"))));
  }

  @Test
  public void shouldExtractAsAliasedDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 AS t;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(new AliasedRelation(TEST1, SourceName.of("T"))));
  }

  @Test
  public void shouldExtractUnaliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 JOIN TEST2"
        + " ON test1.col1 = test2.col1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, TEST1_NAME)));
    assertThat((Join) result.getFrom(), hasRights(new AliasedRelation(TEST2, TEST2_NAME)));
  }

  @Test
  public void shouldExtractMultipleUnaliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 "
        + "JOIN TEST2 ON test1.col1 = test2.col1 "
        + "JOIN TEST3 ON test1.col1 = test3.col1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, TEST1_NAME)));
    assertThat((Join) result.getFrom(), hasRights(
        new AliasedRelation(TEST2, TEST2_NAME),
        new AliasedRelation(TEST3, TEST3_NAME)
    ));
  }

  @Test
  public void shouldHandleAliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 t1 JOIN TEST2 t2"
        + " ON test1.col1 = test2.col1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, SourceName.of("T1"))));
    assertThat((Join) result.getFrom(), hasRights(new AliasedRelation(TEST2, SourceName.of("T2"))));
  }

  @Test
  public void shouldHandleMultipleAliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 t1 "
        + "JOIN TEST2 t2 ON t1.col1 = t2.col1 "
        + "JOIN TEST3 t3 ON t1.col1 = t3.col1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, SourceName.of("T1"))));
    assertThat((Join) result.getFrom(), hasRights(
        new AliasedRelation(TEST2, SourceName.of("T2")),
        new AliasedRelation(TEST3, SourceName.of("T3"))
    ));
  }

  @Test
  public void shouldExtractAsAliasedJoinDataSources() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT * FROM TEST1 AS t1 JOIN TEST2 AS t2"
        + " ON t1.col1 = t2.col1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getFrom(), is(instanceOf(Join.class)));
    assertThat((Join) result.getFrom(), hasLeft(new AliasedRelation(TEST1, SourceName.of("T1"))));
    assertThat((Join) result.getFrom(), hasRights(new AliasedRelation(TEST2, SourceName.of("T2"))));
  }

  @Test
  public void shouldHandleUnqualifiedSelect() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildStatement(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'UNKNOWN' is not a valid stream/table name or alias."));
  }

  @Test
  public void shouldOmitSelectAliasIfNotPresent() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT COL0 FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.empty())))));
  }

  @Test
  public void shouldHandleQualifiedSelectStar() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TEST1.* FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(TEST1_NAME))))));
  }

  @Test
  public void shouldHandleAliasQualifiedSelectStar() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT T.* FROM TEST1 T;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(SourceName.of("T")))))));
  }

  @Test
  public void shouldThrowOnUnknownStarAlias() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT unknown.* FROM TEST1;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildStatement(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString("'UNKNOWN' is not a valid stream/table name or alias."));
  }

  @Test
  public void shouldHandleUnqualifiedSelectStarOnJoin() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

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
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(),
        is(new Select(ImmutableList.of(new AllColumns(Optional.of(SourceName.of("T2")))))));
  }

  @Test
  public void shouldThrowOnUnknownStarAliasOnJoin() {
    // Given:
    final SingleStatementContext stmt = givenQuery(
        "SELECT unknown.* FROM TEST1 JOIN TEST2 WITHIN 1 SECOND ON TEST1.ID = TEST2.ID;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> builder.buildStatement(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString("'UNKNOWN' is not a valid stream/table name or alias."));
  }

  @Test
  public void shouldDefaultToEmptyRefinementForBareQueries() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat("Should be pull", result.isPullQuery(), is(true));
    assertThat(result.getRefinement(), is(Optional.empty()));
  }

  @Test
  public void shouldSupportExplicitEmitChangesOnBareQuery() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1 EMIT CHANGES;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitFinalOnBareQuery() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT * FROM TEST1 EMIT FINAL;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.FINAL));
  }

  @Test
  public void shouldDefaultToEmitChangesForCsas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X AS SELECT * FROM TEST1;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldDefaultToEmitChangesForCtas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X AS SELECT COUNT(1) FROM TEST1 GROUP BY ROWKEY;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldDefaultToEmitChangesForInsertInto() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("INSERT INTO TEST1 SELECT * FROM TEST2;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitChangesForCsas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X AS SELECT * FROM TEST1 EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitFinalForCsas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X AS SELECT * FROM TEST1 EMIT FINAL;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.FINAL));
  }

  @Test
  public void shouldSupportExplicitEmitChangesForCtas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X AS SELECT COUNT(1) FROM TEST1 GROUP BY ROWKEY EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitFinalForCtas() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X AS SELECT COUNT(1) FROM TEST1 GROUP BY ROWKEY EMIT FINAL;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.FINAL));
  }

  @Test
  public void shouldSupportExplicitEmitChangesForInsertInto() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("INSERT INTO TEST1 SELECT * FROM TEST2 EMIT CHANGES;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.CHANGES));
  }

  @Test
  public void shouldSupportExplicitEmitFinalForInsertInto() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("INSERT INTO TEST1 SELECT * FROM TEST2 EMIT FINAL;");

    // When:
    final Query result = ((QueryContainer) builder.buildStatement(stmt)).getQuery();

    // Then:
    assertThat("Should be push", result.isPullQuery(), is(false));
    assertThat(result.getRefinement().get().getOutputRefinement(), is(OutputRefinement.FINAL));
  }

  @Test
  public void shouldSupportQuotedExplainStatements() {
    // Given:
    final SingleStatementContext stmt = givenQuery("EXPLAIN `CSAS_FOO-BAR`;");

    // When:
    final Explain explain = (Explain) builder.buildStatement(stmt);

    // Then:
    assertThat(explain.getQueryId(), is(Optional.of("CSAS_FOO-BAR")));
  }

  private static SingleStatementContext givenQuery(final String sql) {
    final List<ParsedStatement> statements = KsqlParserTestUtil.parse(sql);
    assertThat(statements, hasSize(1));
    return statements.get(0).getStatement();
  }

  private static UnqualifiedColumnReferenceExp column(final String fieldName) {
    return new UnqualifiedColumnReferenceExp(ColumnName.of(fieldName));
  }

  private static QualifiedColumnReferenceExp column(
      final SourceName source,
      final String fieldName
  ) {
    return new QualifiedColumnReferenceExp(source, ColumnName.of(fieldName));
  }
}