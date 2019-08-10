package io.confluent.ksql.parser.rewrite;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Join.Type;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StatementRewriterTest {
  @Mock
  private BiFunction<Expression, Object, Expression> expressionRewriter;
  @Mock
  private BiFunction<AstNode, Object, AstNode> mockRewriter;
  @Mock
  private Optional<NodeLocation> location;
  @Mock
  private Object context;
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
  private QualifiedName qualifiedName;
  @Mock
  private CreateSourceProperties sourceProperties;
  @Mock
  private Query query;
  @Mock
  private Query rewrittenQuery;
  @Mock
  private CreateSourceAsProperties csasProperties;

  private StatementRewriter<Object> rewriter;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    rewriter = new StatementRewriter<>(expressionRewriter, mockRewriter);
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

  private Query givenQuery(
      final Optional<WindowExpression> window,
      final Optional<Expression> where,
      final Optional<GroupBy> groupBy,
      final Optional<Expression> having) {
    when(mockRewriter.apply(select, context)).thenReturn(rewrittenSelect);
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);
    return new Query(location, select, relation, window, where, groupBy, having, optionalInt);
  }

  @Test
  public void shouldRewriteQuery() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Query(
                location,
                rewrittenSelect,
                rewrittenRelation,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithFilter() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.of(expression), Optional.empty(), Optional.empty());
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Query(
                location,
                rewrittenSelect,
                rewrittenRelation,
                Optional.empty(),
                Optional.of(rewrittenExpression),
                Optional.empty(),
                Optional.empty(),
                optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithGroupBy() {
    // Given:
    final GroupBy groupBy = mock(GroupBy.class);
    final GroupBy rewrittenGroupBy = mock(GroupBy.class);
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.of(groupBy), Optional.empty());
    when(mockRewriter.apply(groupBy, context)).thenReturn(rewrittenGroupBy);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Query(
                location,
                rewrittenSelect,
                rewrittenRelation,
                Optional.empty(),
                Optional.empty(),
                Optional.of(rewrittenGroupBy),
                Optional.empty(),
                optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithWindow() {
    // Given:
    final WindowExpression window = mock(WindowExpression.class);
    final WindowExpression rewrittenWindow = mock(WindowExpression.class);
    final Query query =
        givenQuery(Optional.of(window), Optional.empty(), Optional.empty(), Optional.empty());
    when(mockRewriter.apply(window, context)).thenReturn(rewrittenWindow);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Query(
                location,
                rewrittenSelect,
                rewrittenRelation,
                Optional.of(rewrittenWindow),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                optionalInt))
    );
  }

  @Test
  public void shouldRewriteQueryWithHaving() {
    // Given:
    final Query query =
        givenQuery(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(expression));
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(query, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new Query(
                location,
                rewrittenSelect,
                rewrittenRelation,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(rewrittenExpression),
                optionalInt))
    );
  }

  @Test
  public void shouldRewriteSingleColumn() {
    // Given:
    final SingleColumn singleColumn = new SingleColumn(location, expression, "foo");
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(singleColumn, context);

    // Then:
    assertThat(rewritten, equalTo(new SingleColumn(location, rewrittenExpression, "foo")));
  }

  @Test
  public void shouldRewriteAliasedRelation() {
    // Given:
    final AliasedRelation aliasedRelation = new AliasedRelation(location, relation, "alias");
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);

    // When:
    final AstNode rewritten = rewriter.rewrite(aliasedRelation, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new AliasedRelation(location, rewrittenRelation, "alias")));
  }

  private Join givenJoin(final Optional<WithinExpression> within) {
    when(mockRewriter.apply(relation, context)).thenReturn(rewrittenRelation);
    when(mockRewriter.apply(rightRelation, context)).thenReturn(rewrittenRightRelation);
    return new Join(location, Type.LEFT, relation, rightRelation, joinCriteria, within);
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
                Type.LEFT,
                rewrittenRelation,
                rewrittenRightRelation,
                joinCriteria,
                Optional.empty()))
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
                Type.LEFT,
                rewrittenRelation,
                rewrittenRightRelation,
                joinCriteria,
                Optional.of(rewrittenWithinExpression)))
    );
  }

  @Test
  public void shouldRewriteWindowExpression() {
    // Given:
    final KsqlWindowExpression ksqlWindowExpression = mock(KsqlWindowExpression.class);
    final KsqlWindowExpression rewrittenKsqlWindowExpression = mock(KsqlWindowExpression.class);
    final WindowExpression windowExpression =
        new WindowExpression(location, "name", ksqlWindowExpression);
    when(mockRewriter.apply(ksqlWindowExpression, context))
        .thenReturn(rewrittenKsqlWindowExpression);

    // When:
    final AstNode rewritten = rewriter.rewrite(windowExpression, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new WindowExpression(location, "name", rewrittenKsqlWindowExpression))
    );
  }

  private TableElement givenTableElement(final String name) {
    final TableElement element = mock(TableElement.class);
    when(element.getName()).thenReturn(name);
    when(element.getNamespace()).thenReturn(Namespace.VALUE);
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
        qualifiedName,
        TableElements.of(tableElement1, tableElement2),
        false,
        sourceProperties
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
                qualifiedName,
                TableElements.of(rewrittenTableElement1, rewrittenTableElement2),
                false,
                sourceProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteCSAS() {
    final CreateStreamAsSelect csas = new CreateStreamAsSelect(
        location,
        qualifiedName,
        query,
        false,
        csasProperties,
        Optional.empty()
    );
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    final AstNode rewritten = rewriter.rewrite(csas, context);

    assertThat(
        rewritten,
        equalTo(
            new CreateStreamAsSelect(
                location,
                qualifiedName,
                rewrittenQuery,
                false,
                csasProperties,
                Optional.empty()
            )
        )
    );
  }

  @Test
  public void shouldRewriteCSASWithPartitionBy() {
    final CreateStreamAsSelect csas = new CreateStreamAsSelect(
        location,
        qualifiedName,
        query,
        false,
        csasProperties,
        Optional.of(expression)
    );
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);

    final AstNode rewritten = rewriter.rewrite(csas, context);

    assertThat(
        rewritten,
        equalTo(
            new CreateStreamAsSelect(
                location,
                qualifiedName,
                rewrittenQuery,
                false,
                csasProperties,
                Optional.of(rewrittenExpression)
            )
        )
    );
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
        qualifiedName,
        TableElements.of(tableElement1, tableElement2),
        false,
        sourceProperties
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
                qualifiedName,
                TableElements.of(rewrittenTableElement1, rewrittenTableElement2),
                false,
                sourceProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteCTAS() {
    // Given:
    final CreateTableAsSelect ctas = new CreateTableAsSelect(
        location,
        qualifiedName,
        query,
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
                qualifiedName,
                rewrittenQuery,
                false,
                csasProperties
            )
        )
    );
  }

  @Test
  public void shouldRewriteInsertInto() {
    // Given:
    final InsertInto ii = new InsertInto(location, qualifiedName, query, Optional.empty());
    when(mockRewriter.apply(query, context)).thenReturn(rewrittenQuery);

    // When:
    final AstNode rewritten = rewriter.rewrite(ii, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new InsertInto(location, qualifiedName, rewrittenQuery, Optional.empty()))
    );
  }

  @Test
  public void shouldRewriteInsertIntoWithPartitionBy() {
    // Given:
    final InsertInto ii = new InsertInto(location, qualifiedName, query, Optional.of(expression));
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
                qualifiedName,
                rewrittenQuery,
                Optional.of(rewrittenExpression)
            )
        )
    );
  }

  @Test
  public void shouldRewriteGroupBy() {
    // Given:
    final GroupingElement groupingElement1 = mock(GroupingElement.class);
    final GroupingElement groupingElement2 = mock(GroupingElement.class);
    final GroupingElement rewrittenGroupingElement1 = mock(GroupingElement.class);
    final GroupingElement rewrittenGroupingElement2 = mock(GroupingElement.class);
    final GroupBy groupBy = new GroupBy(
        location,
        ImmutableList.of(groupingElement1, groupingElement2)
    );
    when(mockRewriter.apply(groupingElement1, context)).thenReturn(rewrittenGroupingElement1);
    when(mockRewriter.apply(groupingElement2, context)).thenReturn(rewrittenGroupingElement2);

    // When:
    final AstNode rewritten = rewriter.rewrite(groupBy, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new GroupBy(
                location,
                ImmutableList.of(rewrittenGroupingElement1, rewrittenGroupingElement2)
            )
        )
    );
  }

  @Test
  public void shouldRewriteSimpleGroupBy() {
    // Given:
    final Expression expression2 = mock(Expression.class);
    final Expression rewrittenExpression2 = mock(Expression.class);
    final SimpleGroupBy groupBy =
        new SimpleGroupBy(location, ImmutableList.of(expression, expression2));
    when(expressionRewriter.apply(expression, context)).thenReturn(rewrittenExpression);
    when(expressionRewriter.apply(expression2, context)).thenReturn(rewrittenExpression2);

    // When:
    final AstNode rewritten = rewriter.rewrite(groupBy, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new SimpleGroupBy(
                location,
                ImmutableList.of(rewrittenExpression, rewrittenExpression2)
            )
        )
    );
  }
}
