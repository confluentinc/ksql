package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TypeContextUtilTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldGetTypeFromVarchar() {
    // Given:
    final String schemaString = "VARCHAR";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.STRING)));
  }

  @Test
  public void shouldGetTypeFromDecimal() {
    // Given:
    final String schemaString = "DECIMAL(2, 1)";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.decimal(2, 1))));
  }

  @Test
  public void shouldGetTypeFromStringArray() {
    // Given:
    final String schemaString = "ARRAY<VARCHAR>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.STRING))));
  }

  @Test
  public void shouldGetTypeFromIntArray() {
    // Given:
    final String schemaString = "ARRAY<INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromMap() {
    // Given:
    final String schemaString = "MAP<VARCHAR, INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.map(SqlTypes.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromStruct() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.struct().field("A", SqlTypes.STRING).build())));
  }

  @Test
  public void shouldGetTypeFromStructWithTwoFields() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR, B INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlStruct.builder()
        .field("A", SqlTypes.STRING)
        .field("B", SqlTypes.INTEGER)
        .build())));
  }

  @Test
  public void shouldThrowOnUnsupportedTypeSpec() {
    // Given:
    final String schemaString = "SHAKESPEARE";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unknown primitive type: SHAKESPEARE");

    // When:
    TypeContextUtil.getType(schemaString);
  }

  @Test
  public void shouldThrowOnNonIntegerPrecision() {
    // Given:
    final String schemaString = "DECIMAL(.1, 1)";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Value must be integer for command: DECIMAL(PRECISION)");

    // When:
    TypeContextUtil.getType(schemaString);
  }


  @Test
  public void shouldThrowOnNonIntegerScale() {
    // Given:
    final String schemaString = "DECIMAL(1, 1.1)";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Value must be integer for command: DECIMAL(SCALE)");

    // When:
    TypeContextUtil.getType(schemaString);
  }

}