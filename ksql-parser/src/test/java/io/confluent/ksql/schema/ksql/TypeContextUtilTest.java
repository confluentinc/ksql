package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class TypeContextUtilTest {

  @Test
  public void shouldGetTypeFromVarchar() {
    // Given:
    final String schemaString = "VARCHAR";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(PrimitiveType.of(SqlType.STRING)));
  }

  @Test
  public void shouldGetTypeFromStringArray() {
    // Given:
    final String schemaString = "ARRAY<VARCHAR>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(Array.of(PrimitiveType.of(SqlType.STRING))));
  }

  @Test
  public void shouldGetTypeFromIntArray() {
    // Given:
    final String schemaString = "ARRAY<INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(Array.of(PrimitiveType.of(SqlType.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromMap() {
    // Given:
    final String schemaString = "MAP<VARCHAR, INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(Map.of(PrimitiveType.of(SqlType.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromStruct() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(Struct.builder().addField("A", PrimitiveType.of(SqlType.STRING)).build()));
  }

  @Test
  public void shouldGetTypeFromStructWithTwoFields() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR, B INT>";

    // When:
    final Type type = TypeContextUtil.getType(schemaString);

    // Then:
    assertThat(type, is(Struct.builder()
        .addField("A", PrimitiveType.of(SqlType.STRING))
        .addField("B", PrimitiveType.of(SqlType.INTEGER))
        .build()));
  }

  @Test
  public void shouldThrowOnUnsupportedTypeSpec() {
    // Given:
    final String schemaString = "SHAKESPEARE";

    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> TypeContextUtil.getType(schemaString)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown primitive type: SHAKESPEARE"));
  }

}