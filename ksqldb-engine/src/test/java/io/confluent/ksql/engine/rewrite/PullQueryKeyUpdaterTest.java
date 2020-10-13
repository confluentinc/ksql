package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class PullQueryKeyUpdaterTest {

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  private static final Schema SCHEMA_INT = SchemaBuilder.struct().field("a", SchemaBuilder.int32());
  private static final Struct KEY1 = new Struct(SCHEMA_INT).put("a", 1);
  private static final Struct KEY2 = new Struct(SCHEMA_INT).put("a", 2);
  private static final Struct KEY3 = new Struct(SCHEMA_INT).put("a", 3);
  private static final Schema SCHEMA_UNKNOWN
      = SchemaBuilder.struct().field("a", SCHEMA_INT);
  private static final Struct KEY_UNKNOWN = new Struct(SCHEMA_UNKNOWN).put("a", KEY1);

  private static final Schema SCHEMA_STRING =
      SchemaBuilder.struct().field("a", SchemaBuilder.string());
  private static final Struct KEY4 = new Struct(SCHEMA_STRING).put("a", "foo");

  private static final Schema SCHEMA_DOUBLE =
      SchemaBuilder.struct().field("a", SchemaBuilder.float64());
  private static final Struct KEY5 = new Struct(SCHEMA_DOUBLE).put("a", 12.345);

  private static final Schema SCHEMA_FLOAT =
      SchemaBuilder.struct().field("a", SchemaBuilder.float64());
  private static final Struct KEY6 = new Struct(SCHEMA_FLOAT).put("a", 12.345);

  @Test
  public void shouldThrowIfTypeUnknown() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID IN (2);");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> PullQueryKeyUpdater.update(query, ImmutableList.of(KEY_UNKNOWN))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown key type STRUCT"));
  }

  @Test
  public void shouldRewrite_int() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID IN (4, 5, 6, 7, 8, 9);");

    // When:
    final Query rewrittenQuery = (Query) PullQueryKeyUpdater.update(query,
        ImmutableList.of(KEY1, KEY2, KEY3));

    // Then:
    InPredicate inPredicate = (InPredicate) rewrittenQuery.getWhere().get();
    assertThat(inPredicate.getValueList().getValues().size(), is(3));
    IntegerLiteral integerLiteral = (IntegerLiteral) inPredicate.getValueList().getValues().get(0);
    assertThat(integerLiteral.getValue(), is(1));
    integerLiteral = (IntegerLiteral) inPredicate.getValueList().getValues().get(1);
    assertThat(integerLiteral.getValue(), is(2));
    integerLiteral = (IntegerLiteral) inPredicate.getValueList().getValues().get(2);
    assertThat(integerLiteral.getValue(), is(3));
  }

  @Test
  public void shouldRewrite_string() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID IN ('a', 'b', 'c');");

    // When:
    final Query rewrittenQuery = (Query) PullQueryKeyUpdater.update(query,
        ImmutableList.of(KEY4));

    // Then:
    InPredicate inPredicate = (InPredicate) rewrittenQuery.getWhere().get();
    assertThat(inPredicate.getValueList().getValues().size(), is(1));
    StringLiteral stringLiteral = (StringLiteral) inPredicate.getValueList().getValues().get(0);
    assertThat(stringLiteral.getValue(), is("foo"));
  }

  @Test
  public void shouldRewrite_double() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID IN (2.2, 3.3);");

    // When:
    final Query rewrittenQuery = (Query) PullQueryKeyUpdater.update(query,
        ImmutableList.of(KEY5));

    // Then:
    InPredicate inPredicate = (InPredicate) rewrittenQuery.getWhere().get();
    assertThat(inPredicate.getValueList().getValues().size(), is(1));
    DoubleLiteral doubleLiteral = (DoubleLiteral) inPredicate.getValueList().getValues().get(0);
    assertThat(doubleLiteral.getValue(), is(12.345));
  }

  @Test
  public void shouldRewrite_float() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID IN (2.2, 3.3);");

    // When:
    final Query rewrittenQuery = (Query) PullQueryKeyUpdater.update(query,
        ImmutableList.of(KEY5));

    // Then:
    InPredicate inPredicate = (InPredicate) rewrittenQuery.getWhere().get();
    assertThat(inPredicate.getValueList().getValues().size(), is(1));
    DoubleLiteral doubleLiteral = (DoubleLiteral) inPredicate.getValueList().getValues().get(0);
    assertThat(doubleLiteral.getValue(), is(12.345));
  }

  @Test
  public void shouldNotRewrite() {
    // Given:
    final Query query = (Query) givenQuery("SELECT * FROM UNKNOWN WHERE ID = 4;");

    // When:
    final Query rewrittenQuery = (Query) PullQueryKeyUpdater.update(query,
        ImmutableList.of(KEY1, KEY2, KEY3));

    // Then:
    ComparisonExpression comparison = (ComparisonExpression) rewrittenQuery.getWhere().get();
    IntegerLiteral integerLiteral = (IntegerLiteral) comparison.getRight();
    assertThat(integerLiteral.getValue(), is(4));
  }

  private static Statement givenQuery(final String sql) {
    final List<ParsedStatement> statements = new DefaultKsqlParser().parse(sql);
    assertThat(statements, hasSize(1));
    return new AstBuilder(META_STORE).buildStatement(statements.get(0).getStatement());
  }

}
