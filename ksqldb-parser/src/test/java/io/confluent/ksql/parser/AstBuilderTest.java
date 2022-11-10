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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IntervalUnit;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
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
  public void shouldParsePause() {
    // Given:
    final SingleStatementContext allStmt = givenQuery("PAUSE ALL;");
    final SingleStatementContext oneStmt = givenQuery("PAUSE QUERY1;");

    // When:
    final PauseQuery allResult = (PauseQuery) builder.buildStatement(allStmt);
    final PauseQuery oneResult = (PauseQuery) builder.buildStatement(oneStmt);

    // Then:
    assertThat(allResult.getQueryId(), is(Optional.empty()));
    assertThat(oneResult.getQueryId(), is(Optional.of(new QueryId("QUERY1"))));
  }

  @Test
  public void shouldParseResume() {
    // Given:
    final SingleStatementContext allStmt = givenQuery("RESUME ALL;");
    final SingleStatementContext oneStmt = givenQuery("RESUME QUERY1;");

    // When:
    final ResumeQuery allResult = (ResumeQuery) builder.buildStatement(allStmt);
    final ResumeQuery oneResult = (ResumeQuery) builder.buildStatement(oneStmt);

    // Then:
    assertThat(allResult.getQueryId(), is(Optional.empty()));
    assertThat(oneResult.getQueryId(), is(Optional.of(new QueryId("QUERY1"))));
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
  public void shouldHandleSelectStructAll() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT NESTED_ORDER_COL->* FROM NESTED_STREAM;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new StructAll(column("NESTED_ORDER_COL"))
    ))));
  }

  @Test
  public void shouldHandleSelectStructAllOnNestedStruct() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("SELECT NESTED_ORDER_COL->ITEMINFO->* FROM NESTED_STREAM;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new StructAll(
            new DereferenceExpression(
                Optional.empty(),
                new UnqualifiedColumnReferenceExp(ColumnName.of("NESTED_ORDER_COL")),
                "ITEMINFO"
            ))
    ))));
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
  public void shouldBuildLambdaFunction() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TRANSFORM_ARRAY(Col4, X => X + 5) FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TRANSFORM_ARRAY"),
                ImmutableList.of(
                    column("COL4"),
                    new LambdaFunctionCall(
                        ImmutableList.of("X"),
                        new ArithmeticBinaryExpression(
                            Operator.ADD,
                            new LambdaVariable("X"),
                            new IntegerLiteral(5))
                  )
                )
            ),
            Optional.empty())
    ))));
  }

  @Test
  public void shouldBuildLambdaFunctionWithMultipleLambdas() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TRANSFORM_ARRAY(Col4, X => X + 5, (X,Y) => X + Y) FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TRANSFORM_ARRAY"),
                ImmutableList.of(
                    column("COL4"),
                    new LambdaFunctionCall(
                        ImmutableList.of("X"),
                        new ArithmeticBinaryExpression(
                            Operator.ADD,
                            new LambdaVariable("X"),
                            new IntegerLiteral(5))
                    ),
                    new LambdaFunctionCall(
                        ImmutableList.of("X", "Y"),
                        new ArithmeticBinaryExpression(
                            Operator.ADD,
                            new LambdaVariable("X"),
                            new LambdaVariable("Y")
                        )
                    )
                )
            ),
            Optional.empty())
    ))));
  }

  @Test
  public void shouldBuildNestedLambdaFunction() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TRANSFORM_ARRAY(Col4, (X,Y) => TRANSFORM_ARRAY(Col4, X => 5)) FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TRANSFORM_ARRAY"),
                ImmutableList.of(
                    column("COL4"),
                    new LambdaFunctionCall(
                        ImmutableList.of("X", "Y"),
                        new FunctionCall(
                            FunctionName.of("TRANSFORM_ARRAY"),
                            ImmutableList.of(
                                column("COL4"),
                                new LambdaFunctionCall(
                                    ImmutableList.of("X"),
                                    new IntegerLiteral(5)
                                )
                            )
                        )
                    )
                )
            ),
            Optional.empty())
    ))));
  }

  @Test
  public void shouldNotBuildLambdaFunctionNotLastArguments() {
    // Given:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> givenQuery("SELECT TRANSFORM_ARRAY(X => X + 5, Col4) FROM TEST1;")
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("line 1:26: Syntax error at line 1:26")
    );
    assertThat(
        e.getUnloggedMessage(),
        containsString("Syntax error at or near '=>' at line 1:26")
    );
    assertThat(e.getSqlStatement(), containsString("=>"));
  }

  @Test
  public void shouldBuildIntervalUnit() {
    // Given:
    final SingleStatementContext stmt = givenQuery("SELECT TIMESTAMPADD(MINUTES, 5, Col4) FROM TEST1;");

    // When:
    final Query result = (Query) builder.buildStatement(stmt);

    // Then:
    assertThat(result.getSelect(), is(new Select(ImmutableList.of(
        new SingleColumn(
            new FunctionCall(
                FunctionName.of("TIMESTAMPADD"),
                ImmutableList.of(
                    new IntervalUnit(TimeUnit.MINUTES),
                    new IntegerLiteral(5),
                    column("COL4")
                )
            ),
            Optional.empty())
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
  public void shouldCreateSourceTable() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE SOURCE TABLE X WITH (kafka_topic='X');");

    // When:
    final CreateTable result = (CreateTable) builder.buildStatement(stmt);

    // Then:
    assertThat(result.isSource(), is(true));
  }

  @Test
  public void shouldCreateNormalTable() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE TABLE X WITH (kafka_topic='X');");

    // When:
    final CreateTable result = (CreateTable) builder.buildStatement(stmt);

    // Then:
    assertThat(result.isSource(), is(false));
  }

  public void shouldCreateSourceStream() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE SOURCE STREAM X WITH (kafka_topic='X');");

    // When:
    final CreateStream result = (CreateStream) builder.buildStatement(stmt);

    // Then:
    assertThat(result.isSource(), is(true));
  }

  @Test
  public void shouldCreateNormalStream() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("CREATE STREAM X WITH (kafka_topic='X');");

    // When:
    final CreateStream result = (CreateStream) builder.buildStatement(stmt);

    // Then:
    assertThat(result.isSource(), is(false));
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

  @Test
  public void shouldFailOnStringWithParameter() {
     assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K STRING (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnIntegerWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K INTEGER (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnVarcharWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K VARCHAR (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnIntWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K INT (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnDoubleWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K DOUBLE (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnBooleanWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K BOOLEAN (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldFailOnBigIntWithParameter() {
    assertThrows(
            ParseFailedException.class,
            () -> givenQuery("CREATE STREAM INPUT (K BIGINT (KEY)) WITH (kafka_topic='input',value_format='JSON');")
    );
  }

  @Test
  public void shouldSupportHeadersColumns() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("CREATE STREAM INPUT (K ARRAY<STRUCT<key STRING, value BYTES>> HEADERS) WITH (kafka_topic='input',value_format='JSON');");

    // When:
    final CreateStream createStream = (CreateStream) builder.buildStatement(stmt);

    // Then:
    final TableElement column = Iterators.getOnlyElement(createStream.getElements().iterator());
    assertThat(column.getConstraints(), is((new ColumnConstraints.Builder()).headers().build()));
  }

  @Test
  public void shouldSupportSingleHeaderColumn() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("CREATE STREAM INPUT (K BYTES HEADER('h1')) WITH (kafka_topic='input',value_format='JSON');");

    // When:
    final CreateStream createStream = (CreateStream) builder.buildStatement(stmt);

    // Then:
    final TableElement column = Iterators.getOnlyElement(createStream.getElements().iterator());
    assertThat(column.getConstraints(),
        is((new ColumnConstraints.Builder()).header("h1").build()));
  }

  @Test
  public void shouldRejectIncorrectlyTypedHeadersColumns() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("CREATE STREAM INPUT (K BIGINT HEADERS) WITH (kafka_topic='input',value_format='JSON');");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> {
      builder.buildStatement(stmt);
    });

    // Then:
    assertThat(e.getMessage(), is("Invalid type for HEADERS column: expected ARRAY<STRUCT<`KEY` STRING, `VALUE` BYTES>>, got BIGINT"));
  }

  @Test
  public void shouldRejectIncorrectlyTypedSingleHeaderColumns() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("CREATE STREAM INPUT (K BIGINT HEADER('abc')) WITH (kafka_topic='input',value_format='JSON');");

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> {
      builder.buildStatement(stmt);
    });

    // Then:
    assertThat(e.getMessage(), is("Invalid type for HEADER('abc') column: expected BYTES, got BIGINT"));
  }

  @Test
  public void shouldFailOnPersistentQueryLimitClauseStream() {
    // Given:
    final SingleStatementContext stmt
            = givenQuery("CREATE STREAM X AS SELECT * FROM TEST1 LIMIT 5;");

    // Then:
    Exception exception = assertThrows(KsqlException.class, () -> {
      builder.buildStatement(stmt);
    });

    String expectedMessage = "CREATE STREAM AS SELECT statements don't support LIMIT clause.";
    String actualMessage = exception.getMessage();

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldFailOnPersistentQueryLimitClauseTable() {
    // Given:
    final SingleStatementContext stmt
            = givenQuery("CREATE TABLE X AS SELECT * FROM TEST1 LIMIT 5;");

    // Then:
    Exception exception = assertThrows(KsqlException.class, () -> {
      builder.buildStatement(stmt);
    });

    String expectedMessage = "CREATE TABLE AS SELECT statements don't support LIMIT clause.";
    String actualMessage = exception.getMessage();

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldBuildAssertTopic() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("ASSERT TOPIC X;");

    // When:
    final AssertTopic assertTopic = (AssertTopic) builder.buildStatement(stmt);

    // Then:
    assertThat(assertTopic.getTopic(), is("X"));
    assertThat(assertTopic.getConfig().size(), is(0));
    assertThat(assertTopic.getTimeout(), is(Optional.empty()));
    assertThat(assertTopic.checkExists(), is(true));
  }

  @Test
  public void shouldBuildAssertNotExistsTopicWithConfigsAndTimeout() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("ASSERT NOT EXISTS TOPIC 'a-b-c' WITH (REPLICAS=1, partitions=1) TIMEOUT 10 SECONDS;");

    // When:
    final AssertTopic assertTopic = (AssertTopic) builder.buildStatement(stmt);

    // Then:
    assertThat(assertTopic.getTopic(), is("a-b-c"));
    assertThat(assertTopic.getConfig().get("REPLICAS").getValue(), is(1));
    assertThat(assertTopic.getConfig().get("PARTITIONS").getValue(), is(1));
    assertThat(assertTopic.getTimeout().get().getTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(assertTopic.getTimeout().get().getValue(), is(10L));
    assertThat(assertTopic.checkExists(), is(false));
  }

  @Test
  public void shouldBuildAssertSchemaWithSubject() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("ASSERT SCHEMA SUBJECT X;");

    // When:
    final AssertSchema assertSchema = (AssertSchema) builder.buildStatement(stmt);

    // Then:
    assertThat(assertSchema.getSubject(), is(Optional.of("X")));
    assertThat(assertSchema.getId(), is(Optional.empty()));
    assertThat(assertSchema.getTimeout(), is(Optional.empty()));
    assertThat(assertSchema.checkExists(), is(true));
  }

  @Test
  public void shouldBuildAssertNotExistsSchemaWithIdAndTimeout() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("ASSERT NOT EXISTS SCHEMA ID 24 TIMEOUT 10 SECONDS;");

    // When:
    final AssertSchema assertSchema = (AssertSchema) builder.buildStatement(stmt);

    // Then:
    assertThat(assertSchema.getSubject(), is(Optional.empty()));
    assertThat(assertSchema.getId(), is(Optional.of(24)));
    assertThat(assertSchema.getTimeout().get().getTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(assertSchema.checkExists(), is(false));
  }

  @Test
  public void shouldBuildAssertNotExistsWithSubjectAndId() {
    // Given:
    final SingleStatementContext stmt
        = givenQuery("ASSERT NOT EXISTS SCHEMA SUBJECT 'a-b-c' ID 33;");

    // When:
    final AssertSchema assertSchema = (AssertSchema) builder.buildStatement(stmt);

    // Then:
    assertThat(assertSchema.getSubject(), is(Optional.of("a-b-c")));
    assertThat(assertSchema.getId(), is(Optional.of(33)));
    assertThat(assertSchema.getTimeout(), is(Optional.empty()));
    assertThat(assertSchema.checkExists(), is(false));
  }

  @Test
  public void shouldThrowOnNoSubjectOrId() {
    // Given:
    final SingleStatementContext stmt = givenQuery("ASSERT SCHEMA TIMEOUT 10 SECONDS;");

    // When:
    final Exception e = assertThrows(KsqlException.class, () -> builder.buildStatement(stmt));

    // Then:
    assertThat(e.getMessage(), is("ASSERT SCHEMA statements much include a subject name or an id"));
  }

  @Test
  public void shouldThrowOnNonIntegerId() {
    // Given:
    final SingleStatementContext stmt =
        givenQuery("ASSERT SCHEMA ID FALSE TIMEOUT 10 SECONDS;");

    // When:
    final Exception e = assertThrows(KsqlException.class, () -> builder.buildStatement(stmt));

    // Then:
    assertThat(e.getMessage(), is("ID must be an integer"));
  }

  @Test
  public void shouldGetCorrectLocationsSimpleCtasMultiLine() {
    // Given:
    final String query =
        "CREATE TABLE X AS\n" +
        "  SELECT COUNT(1)\n" +
        "  FROM TEST1\n" +
        "  GROUP BY ROWKEY;";
    final SingleStatementContext stmt = givenQuery(query);

    // When:
    final CreateTableAsSelect result = (CreateTableAsSelect) builder.buildStatement(stmt);

    // Then:
    final NodeLocation loc = result.getLocation().get();
    final TokenLocation start = loc.getStartTokenLocation();
    final TokenLocation stop = loc.getStopTokenLocation().get();

    assertThat(start.getLine(), is(1));
    assertThat(start.getCharPositionInLine(), is(0));
    assertThat(start.getStartIndex(), is(0));
    assertThat(start.getStopIndex(),
        is("CREATE TABLE X AS".indexOf("E TABLE"))); // ends at the E of CREATE


    assertThat(stop.getLine(), is(4));

    // last token is ROWKEY
    assertThat(stop.getCharPositionInLine(),
        is("  GROUP BY ROWKEY;".indexOf("ROWKEY")));
    assertThat(stop.getStartIndex(),
        is("CREATE TABLE X AS\n  SELECT COUNT(1)\n  FROM TEST1\n  GROUP BY ROWKEY;".indexOf("ROWKEY")));
    assertThat(stop.getStopIndex(),
        is("CREATE TABLE X AS\n  SELECT COUNT(1)\n  FROM TEST1\n  GROUP BY ROWKEY;".indexOf("Y;")));

    assertThat(loc.getLength(), is(OptionalInt.of(query.length() - 1))); // subtracting 1 as the last ';' is not counted
  }

  @Test
  public void shouldGetCorrectLocationsComplexCtas() {
    // Given:
    final String statementString =
    //   1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
        "CREATE TABLE customer_bookings\n" +                                                               // 1
        "  WITH (KAFKA_TOPIC = 'customer_bookings', KEY_FORMAT = 'JSON', VALUE_FORMAT = 'JSON') AS\n" +    // 2
        "  SELECT C.EMAIL,\n" +                                                                            // 3
        "         B.id,\n" +                                                                               // 4
        "         B.flight_id,\n" +                                                                        // 5
        "         COUNT(*)\n" +                                                                            // 6
        "  FROM bookings B\n" +                                                                            // 7
        "       INNER JOIN customers C ON B.customer_id = C.id\n" +                                        // 8
        "  WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)\n" +                                        // 9
        "  WHERE B.customer_id > 0 AND INSTR(C.EMAIL, '@') > 0\n" +                                        // 10
        "  GROUP BY C.EMAIL, B.ID, B.flight_id\n" +                                                        // 11
        "  HAVING COUNT(*) > 0;";                                                                          // 12
    //   1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
    final SingleStatementContext stmt = givenQuery(statementString);

    // When:
    final CreateTableAsSelect result = ((CreateTableAsSelect) builder.buildStatement(stmt));

    // Then:
    assertTrue(result.getLocation().isPresent());
    final NodeLocation createTableLocation = result.getLocation().get();
    assertThat(createTableLocation.getStartLineNumber(), is(1));
    assertThat(createTableLocation.getLength(), is(OptionalInt.of(statementString.length() - 1)));

    final Query query = result.getQuery();
    assertTrue(query.getLocation().isPresent());
    final NodeLocation queryLocation = query.getLocation().get();
    assertThat(queryLocation.getStartLineNumber(), is(3));
    assertThat(queryLocation.getStartColumnNumber(), is(3));
    assertThat(queryLocation.getLength(),
        is(OptionalInt.of((
            "SELECT C.EMAIL,\n" +
            "         B.id,\n" +
            "         B.flight_id,\n" +
            "         COUNT(*)\n" +
            "  FROM bookings B\n" +
            "       INNER JOIN customers C ON B.customer_id = C.id\n" +
            "  WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)\n" +
            "  WHERE B.customer_id > 0 AND INSTR(C.EMAIL, '@') > 0\n" +
            "  GROUP BY C.EMAIL, B.ID, B.flight_id\n" +
            "  HAVING COUNT(*) > 0").length())));

    final Select select = query.getSelect();
    assertTrue(select.getLocation().isPresent());
    final NodeLocation selectLocation = select.getLocation().get();
    assertThat(selectLocation.getStartLineNumber(), is(3));
    assertThat(selectLocation.getStartColumnNumber(), is(3));
    assertThat(selectLocation.getLength(),
        is(OptionalInt.of("SELECT".length())));

    final Join join = (Join) query.getFrom();
    assertTrue(join.getLocation().isPresent());
    final NodeLocation joinLocation = join.getLocation().get();
    assertThat(joinLocation.getStartLineNumber(), is(7));
    assertThat(joinLocation.getStartColumnNumber(), is(8));
    assertThat(joinLocation.getLength(),
        is(OptionalInt.of((
                   "bookings B\n" +
            "       INNER JOIN customers C ON B.customer_id = C.id").length())));

    assertTrue(query.getWindow().isPresent());
    final WindowExpression window = query.getWindow().get();
    assertTrue(window.getLocation().isPresent());
    final NodeLocation windowLocation = window.getLocation().get();
    assertThat(windowLocation.getStartLineNumber(), is(9));
    assertThat(windowLocation.getStartColumnNumber(), is(10));
    assertThat(windowLocation.getLength(),
        is(OptionalInt.of(("TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS)").length())));

    assertTrue(query.getWhere().isPresent());
    final LogicalBinaryExpression where = (LogicalBinaryExpression) query.getWhere().get();
    assertTrue(where.getLocation().isPresent());
    final NodeLocation whereLocation = where.getLocation().get();
    assertThat(whereLocation.getStartLineNumber(), is(10));
    assertThat(whereLocation.getStartColumnNumber(), is(27));
    assertThat(whereLocation.getLength(), is(OptionalInt.of(3)));

    assertTrue(query.getGroupBy().isPresent());
    final GroupBy groupBy = query.getGroupBy().get();
    assertTrue(groupBy.getLocation().isPresent());
    final NodeLocation groupByLocation = groupBy.getLocation().get();
    assertThat(groupByLocation.getStartLineNumber(), is(11));
    assertThat(groupByLocation.getStartColumnNumber(), is(12));
    assertThat(groupByLocation.getLength(),
        is(OptionalInt.of("C.EMAIL, B.ID, B.flight_id".length())));

    assertTrue(query.getHaving().isPresent());
    final ComparisonExpression having = (ComparisonExpression) query.getHaving().get();
    assertTrue(having.getLocation().isPresent());
    final NodeLocation havingLocation = having.getLocation().get();
    assertThat(havingLocation.getStartLineNumber(), is(12));
    assertThat(havingLocation.getStartColumnNumber(), is(19));
    assertThat(havingLocation.getLength(), is(OptionalInt.of(1)));
  }
}