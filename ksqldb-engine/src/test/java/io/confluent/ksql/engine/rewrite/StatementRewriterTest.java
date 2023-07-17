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

package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.rewrite.StatementRewriter.Context;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.InsertIntoProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.JoinedSource.Type;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.serde.RefinementInfo;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StatementRewriterTest {

  @Mock
  private BiFunction<Expression, Object, Expression> expressionRewriter;
  @Mock
  private BiFunction<AstNode, Object, AstNode> mockRewriter;
  @Mock
  private BiFunction<AstNode, Context<Object>, Optional<AstNode>> mockPlugin;
  @Mock
  private Optional<NodeLocation> location;
  @Mock
  private Object context;
  @Mock
  private SourceName sourceName;
  @Mock
  private Select select;
  @Mock
  private Select rewrittenSelect;
  @Mock
  private Relation relation;
  @Mock
  private Relation rewrittenRelation;
  @Mock
  private Expression expression;
  @Mock
  private Expression rewrittenExpression;
  @Mock
  private OptionalInt optionalInt;
  @Mock
  private Relation rightRelation;
  @Mock
  private Relation rewrittenRightRelation;
  @Mock
  private JoinCriteria joinCriteria;
  @Mock
  private CreateSourceProperties sourceProperties;
  @Mock
  private Query query;
  @Mock
  private Query rewrittenQuery;
  @Mock
  private CreateSourceAsProperties csasProperties;
  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private InsertIntoProperties insertIntoProperties;
  @Captor
  private ArgumentCaptor<Context<Object>> contextCaptor;

  private StatementRewriter<Object> rewriter;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(mockPlugin.apply(any(), any())).thenReturn(Optional.empty());
    rewriter = new StatementRewriter<>(expressionRewriter, mockPlugin, mockRewriter);
  }

  @Test
  public void shouldRewriteStatements() {
    // Given:
    final Statement statement1 = mock(Statement.class);
    final Statement statement2 = mock(Statement.class);
    final Statement rewritten1 = mock(Statement.class);
    final Statement rewritten2 = mock(Statement.class);
    final Statements statements =
        new Statements(location, ImmutableList.of(statement1, statement2));
    when(mockRewriter.apply(statement1, context)).thenReturn(rewritten1);
    when(mockRewriter.apply(statement2, context)).thenReturn(rewritten2);

    // When:
    final AstNode rewritten = rewriter.rewrite(statements, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new Statements(location, ImmutableList.of(rewritten1, rewritten2)))
    );
  }

  @Test
  public void shouldUsePluginToRewriteStatements() {
    // Given:
    final Statements statements = mock(Statements.class);
    final Statements pluginResult = mock(Statements.class);
    when(statements.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(statements, pluginResult);
  }

  private Query givenQuery(
      final Optional<WindowExpression> window,
      final Optional<Expression> where,
      final Optional<GroupBy> groupBy,
      final Optional<PartitionBy> partitionBy,
      final Optional<Expression> having
  ) {
    when(mockRewriter.apply(select, context)).thenReturn(rewrittenSelect);
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);
    return new Query(
        location,
        select,
        relation,
        window,
        where,
        groupBy,
        partitionBy,
        having,
        Optional.of(refinementInfo),
        false,
        optionalInt
    );
  }

  @Test
  public void shouldRewriteQuery() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryUsingPlugin() {
    // Given:
    final Query query = mock(Query.class);
    final Query pluginResult = mock(Query.class);
    when(query.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(query, pluginResult);
  }

  @Test
  public void shouldRewriteQueryWithFilter() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.of(expression), Optional.empty(), Optional.empty(), Optional.empty());
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.empty(),
        Optional.of(rewrittenExpression),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithGroupBy() {
    // Given:
    final GroupBy groupBy = mock(GroupBy.class);
    final GroupBy rewrittenGroupBy = mock(GroupBy.class);
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.of(groupBy), Optional.empty(), Optional.empty());
    when(mockRewriter.apply(groupBy, context)).thenReturn(rewrittenGroupBy);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.empty(),
        Optional.empty(),
        Optional.of(rewrittenGroupBy),
        Optional.empty(),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithPartitionBy() {
    // Given:
    final PartitionBy partitionBy = mock(PartitionBy.class);
    final PartitionBy rewrittenPartitionBy = mock(PartitionBy.class);

    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(partitionBy),
            Optional.empty());

    when(mockRewriter.apply(partitionBy, context)).thenReturn(rewrittenPartitionBy);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(rewrittenPartitionBy),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithWindow() {
    // Given:
    final WindowExpression window = mock(WindowExpression.class);
    final WindowExpression rewrittenWindow = mock(WindowExpression.class);
    final Query query =
        givenQuery(Optional.of(window), Optional.empty(), Optional.empty(),
             Optional.empty(), Optional.empty());
    when(mockRewriter.apply(window, context)).thenReturn(rewrittenWindow);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.of(rewrittenWindow),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithHaving() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(expression));
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(rewritten, equalTo(new Query(
        location,
        rewrittenSelect,
        rewrittenRelation,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(rewrittenExpression),
        Optional.of(refinementInfo),
        false,
        optionalInt))
    );
  }

  @Test
  public void shouldRewriteSingleColumn() {
    // Given:
    final SingleColumn singleColumn = new SingleColumn(
        location,
        expression,
        Optional.of(ColumnName.of("foo"))
    );
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(singleColumn, context);

    // Then:
    assertThat(rewritten, equalTo(new SingleColumn(
        location, rewrittenExpression,
        Optional.of(ColumnName.of("foo")))
    ));
  }

  @Test
  public void shouldRewriteSingleColumnUsingPlugin() {
    // Given:
    final SingleColumn singleColumn = mock(SingleColumn.class);
    final SingleColumn pluginResult = mock(SingleColumn.class);
    when(singleColumn.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(singleColumn, pluginResult);
  }

  @Test
  public void shouldRewriteAliasedRelation() {
    // Given:
    final AliasedRelation aliasedRelation = new AliasedRelation(location, relation, SourceName.of("alias"));
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);

    // When:
    final AstNode rewritten = rewriter.rewrite(aliasedRelation, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new AliasedRelation(location, rewrittenRelation, SourceName.of("alias"))));
  }

  @Test
  public void shouldRewriteAliasedRelationUsingPlugin() {
    // Given:
    final AliasedRelation relation = mock(AliasedRelation.class);
    final AliasedRelation pluginResult = mock(AliasedRelation.class);
    when(relation.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(relation, pluginResult);
  }

  private Join givenJoin(final Optional<WithinExpression> within) {
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);
    when(mockRewriter.apply(rightRelation, context)).thenReturn(rewrittenRightRelation);
    return new Join(location, relation, ImmutableList.of(new JoinedSource(Optional.empty(), rightRelation, Type.LEFT, joinCriteria, within)));
  }

  @Test
  public void shouldRewriteJoin() {
    // Given:
    final Join join = givenJoin(Optional.empty());

    // When:
    final AstNode rewritten = rewriter.rewrite(join, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Join(
                location,
                rewrittenRelation,
                ImmutableList.of(new JoinedSource(
                    Optional.empty(),
                    rewrittenRightRelation,
                    Type.LEFT,
                    joinCriteria,
                    Optional.empty()))))
    );
  }

  @Test
  public void shouldRewriteJoinWithWindowExpression() {
    // Given:
    final WithinExpression withinExpression = mock(WithinExpression.class);
    final WithinExpression rewrittenWithinExpression = mock(WithinExpression.class);
    final Join join = givenJoin(Optional.of(withinExpression));
    when(mockRewriter.apply(withinExpression, context)).thenReturn(rewrittenWithinExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(join, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Join(
                location,
                rewrittenRelation,
                ImmutableList.of(new JoinedSource(
                    Optional.empty(),
                    rewrittenRightRelation,
                    Type.LEFT,
                    joinCriteria,
                    Optional.of(rewrittenWithinExpression)))))
    );
  }

  @Test
  public void shouldRewriteJoinUsingPlugin() {
    // Given:
    final Join join = mock(Join.class);
    final Join pluginResult = mock(Join.class);
    when(join.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(join, pluginResult);
  }

  @Test
  public void shouldRewriteWindowExpression() {
    // Given:
    final KsqlWindowExpression ksqlWindowExpression = mock(KsqlWindowExpression.class);
    final WindowExpression windowExpression =
        new WindowExpression(location, "name", ksqlWindowExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(windowExpression, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new WindowExpression(location, "name", ksqlWindowExpression))
    );
  }

  @Test
  public void shouldRewriteWindowExpressionUsingPlugin() {
    // Given:
    final WindowExpression windowExpression = mock(WindowExpression.class);
    final WindowExpression pluginResult = mock(WindowExpression.class);
    when(windowExpression.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(windowExpression, pluginResult);
  }

  private static TableElement givenTableElement(final String name) {
    final TableElement element = mock(TableElement.class);
    when(element.getName()).thenReturn(ColumnName.of(name));
    when(element.getConstraints()).thenReturn(ColumnConstraints.NO_COLUMN_CONSTRAINTS);
    return element;
  }

  @Test
  public void shouldRewriteCreateStream() {
    // Given:
    final TableElement tableElement1 = givenTableElement("foo");
    final TableElement tableElement2 = givenTableElement("bar");
    final TableElement rewrittenTableElement1 = givenTableElement("baz");
    final TableElement rewrittenTableElement2 = givenTableElement("boz");
    final CreateStream cs = new CreateStream(
        location,
        sourceName,
        TableElements.of(tableElement1, tableElement2),
        false,
        false,
        sourceProperties,
        false
    );
    when(mockRewriter.apply(tableElement1, context)).thenReturn(rewrittenTableElement1);
    when(mockRewriter.apply(tableElement2, context)).thenReturn(rewrittenTableElement2);

    // When:
    final AstNode rewritten = rewriter.rewrite(cs, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new CreateStream(
                location,
                sourceName,
                TableElements.of(rewrittenTableElement1, rewrittenTableElement2),
                false,
                false,
                sourceProperties,
                false
            )
        )
    );
  }

  @Test
  public void shouldRewriteCreateStreamUsingPlugin() {
    // Given:
    final CreateStream cs = mock(CreateStream.class);
    final CreateStream pluginResult = mock(CreateStream.class);
    when(cs.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(cs, pluginResult);
  }

  @Test
  public void shouldRewriteCSAS() {
    final CreateStreamAsSelect csas = new CreateStreamAsSelect(
        location,
        sourceName,
        query,
        false,
        false,
        csasProperties
    );
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    final AstNode rewritten = rewriter.rewrite(csas, context);

    assertThat(
        rewritten,
        equalTo(
            new CreateStreamAsSelect(
                location,
                sourceName,
                rewrittenQuery,
                false,
                false,
                csasProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteCSASWithPartitionBy() {
    final CreateStreamAsSelect csas = new CreateStreamAsSelect(
        location,
        sourceName,
        query,
        false,
        false,
        csasProperties
    );
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    final AstNode rewritten = rewriter.rewrite(csas, context);

    assertThat(
        rewritten,
        equalTo(
            new CreateStreamAsSelect(
                location,
                sourceName,
                rewrittenQuery,
                false,
                false,
                csasProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteCreateStreamAsSelectUsingPlugin() {
    // Given:
    final CreateStreamAsSelect csas = mock(CreateStreamAsSelect.class);
    final CreateStreamAsSelect pluginResult = mock(CreateStreamAsSelect.class);
    when(csas.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(csas, pluginResult);
  }

  @Test
  public void shouldRewriteCreateTable() {
    // Given:
    final TableElement tableElement1 = givenTableElement("foo");
    final TableElement tableElement2 = givenTableElement("bar");
    final TableElement rewrittenTableElement1 = givenTableElement("baz");
    final TableElement rewrittenTableElement2 = givenTableElement("boz");
    final CreateTable ct = new CreateTable(
        location,
        sourceName,
        TableElements.of(tableElement1, tableElement2),
        false,
        false,
        sourceProperties,
        false
    );
    when(mockRewriter.apply(tableElement1, context)).thenReturn(rewrittenTableElement1);
    when(mockRewriter.apply(tableElement2, context)).thenReturn(rewrittenTableElement2);

    // When:
    final AstNode rewritten = rewriter.rewrite(ct, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new CreateTable(
                location,
                sourceName,
                TableElements.of(rewrittenTableElement1, rewrittenTableElement2),
                false,
                false,
                sourceProperties,
                false
            )
        )
    );
  }

  @Test
  public void shouldRewriteCreateTableUsingPlugin() {
    // Given:
    final CreateTable ct = mock(CreateTable.class);
    final CreateTable pluginResult = mock(CreateTable.class);
    when(ct.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(ct, pluginResult);
  }

  @Test
  public void shouldRewriteCTAS() {
    // Given:
    final CreateTableAsSelect ctas = new CreateTableAsSelect(
        location,
        sourceName,
        query,
        false,
        false,
        csasProperties
    );
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    // When:
    final AstNode rewritten = rewriter.rewrite(ctas, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new CreateTableAsSelect(
                location,
                sourceName,
                rewrittenQuery,
                false,
                false,
                csasProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteCreateTableAsSelectUsingPlugin() {
    // Given:
    final CreateTableAsSelect ctas = mock(CreateTableAsSelect.class);
    final CreateTableAsSelect pluginResult = mock(CreateTableAsSelect.class);
    when(ctas.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(ctas, pluginResult);
  }

  @Test
  public void shouldRewriteInsertInto() {
    // Given:
    final InsertInto ii = new InsertInto(location, sourceName, query, insertIntoProperties);
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    // When:
    final AstNode rewritten = rewriter.rewrite(ii, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new InsertInto(location, sourceName, rewrittenQuery, insertIntoProperties))
    );
  }

  @Test
  public void shouldRewriteInsertIntoWithPartitionBy() {
    // Given:
    final InsertInto ii = new InsertInto(location, sourceName, query, insertIntoProperties);
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(ii, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new InsertInto(
                location,
                sourceName,
                rewrittenQuery,
                insertIntoProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteInsertIntoUsingPlugin() {
    // Given:
    final InsertInto ii = mock(InsertInto.class);
    final InsertInto pluginResult = mock(InsertInto.class);
    when(ii.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(ii, pluginResult);
  }

  @Test
  public void shouldRewriteGroupBy() {
    // Given:
    final Expression exp1 = mock(Expression.class);
    final Expression exp2 = mock(Expression.class);
    final Expression rewrittenExp1 = mock(Expression.class);
    final Expression rewrittenExp2 = mock(Expression.class);
    final GroupBy groupBy = new GroupBy(
        location,
        ImmutableList.of(exp1, exp2)
    );
    when(expressionRewriter.apply(exp1, context)).thenReturn(rewrittenExp1);
    when(expressionRewriter.apply(exp2, context)).thenReturn(rewrittenExp2);

    // When:
    final AstNode rewritten = rewriter.rewrite(groupBy, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new GroupBy(
                location,
                ImmutableList.of(rewrittenExp1, rewrittenExp2)
            )
        )
    );
  }

  @Test
  public void shouldRewritePartitionBy() {
    // Given:
    final PartitionBy partitionBy = new PartitionBy(
        location,
        ImmutableList.of(expression)
    );
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(partitionBy, context);

    // Then:
    assertThat(rewritten, equalTo(new PartitionBy(
        location,
        ImmutableList.of(rewrittenExpression)
    )));
  }

  @Test
  public void shouldRewriteExplainWithQuery() {
    // Given:
    final Explain explain = new Explain(location, Optional.empty(), Optional.of(query));
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    // When:
    final AstNode rewritten = rewriter.rewrite(explain, context);

    // Then:
    assertThat(rewritten, is(new Explain(
        location,
        Optional.empty(),
        Optional.of(rewrittenQuery)
    )));
  }

  @Test
  public void shouldNotRewriteExplainWithId() {
    // Given:
    final Explain explain = new Explain(location, Optional.of("id"), Optional.empty());

    // When:
    final AstNode rewritten = rewriter.rewrite(explain, context);

    // Then:
    assertThat(rewritten, is(sameInstance(explain)));
  }

  @Test
  public void shouldRewriteExplainUsingPlugin() {
    // Given:
    final Explain explain = mock(Explain.class);
    final Explain pluginResult = mock(Explain.class);
    when(explain.accept(any(), any())).thenCallRealMethod();

    // When/Then:
    shouldUsePluginToRewrite(explain, pluginResult);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  private void shouldUsePluginToRewrite(final AstNode original, final AstNode pluginResult) {
    // Given:
    when(mockPlugin.apply(any(), any())).thenReturn(Optional.of(pluginResult));

    // When:
    final AstNode rewritten = rewriter.rewrite(original, context);

    // Then:
    assertThat(rewritten, is(pluginResult));
    verify(mockPlugin).apply(same(original), contextCaptor.capture());
    final Context<Object> wrapped = contextCaptor.getValue();
    assertThat(wrapped.getContext(), is(this.context));
  }
}
