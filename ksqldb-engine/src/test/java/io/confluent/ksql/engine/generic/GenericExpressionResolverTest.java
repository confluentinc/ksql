package io.confluent.ksql.engine.generic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * NOTE: most of the funcitonal test coverage is in DefaultSqlValueCoercerTest
 * or ExpressionMetadataTest, this test class just covers the functionality
 * in the GenericExpressionResolver
 */
public class GenericExpressionResolverTest {

  private static final ColumnName FIELD_NAME = ColumnName.of("FOO");

  private final FunctionRegistry registry = TestFunctionRegistry.INSTANCE.get();
  private final KsqlConfig config = new KsqlConfig(ImmutableMap.of());

  @Test
  public void shouldResolveArbitraryExpressions() {
    // Given:
    final SqlType type = SqlTypes.struct().field("FOO", SqlTypes.STRING).build();
    final Expression exp = new CreateStructExpression(ImmutableList.of(
        new Field("FOO", new FunctionCall(
            FunctionName.of("CONCAT"),
            ImmutableList.of(
                new StringLiteral("bar"),
                new StringLiteral("baz"))
        ))
    ));

    // When:
    final Object o = new GenericExpressionResolver(type, FIELD_NAME, registry, config, "insert value").resolve(exp);

    // Then:
    assertThat(o, is(new Struct(
        SchemaBuilder.struct().field("FOO", Schema.OPTIONAL_STRING_SCHEMA).optional().build()
    ).put("FOO", "barbaz")));
  }

  @Test
  public void shouldResolveNullLiteral() {
    // Given:
    final SqlType type = SqlTypes.STRING;
    final Expression exp = new NullLiteral();

    // When:
    final Object o = new GenericExpressionResolver(type, FIELD_NAME, registry, config, "insert value").resolve(exp);

    // Then:
    assertThat(o, Matchers.nullValue());
  }

  @Test
  public void shouldThrowIfCannotCoerce() {
    // Given:
    final SqlType type = SqlTypes.array(SqlTypes.INTEGER);
    final Expression exp = new IntegerLiteral(1);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new GenericExpressionResolver(type, FIELD_NAME, registry, config, "insert value").resolve(exp));

    // Then:
    assertThat(e.getMessage(), containsString("Expected type ARRAY<INTEGER> for field `FOO` but got INTEGER(1)"));
  }


}