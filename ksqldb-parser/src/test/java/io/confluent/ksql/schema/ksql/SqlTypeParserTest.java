package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.util.KsqlStatementException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SqlTypeParserTest {
  @Mock
  private TypeRegistry typeRegistry;
  private SqlTypeParser parser;

  @Before
  public void setUp() {
    parser = SqlTypeParser.create(typeRegistry);
  }

  @Test
  public void shouldGetTypeFromVarchar() {
    // Given:
    final String schemaString = "VARCHAR";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.STRING)));
  }

  @Test
  public void shouldGetTypeFromDecimal() {
    // Given:
    final String schemaString = "DECIMAL(2, 1)";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.decimal(2, 1))));
  }

  @Test
  public void shouldGetTypeFromStringArray() {
    // Given:
    final String schemaString = "ARRAY<VARCHAR>";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.STRING))));
  }

  @Test
  public void shouldGetTypeFromIntArray() {
    // Given:
    final String schemaString = "ARRAY<INT>";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromMap() {
    // Given:
    final String schemaString = "MAP<BIGINT, INT>";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))));
  }

  @Test
  public void shouldGetTypeFromStruct() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR>";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.struct().field("A", SqlTypes.STRING).build())));
  }

  @Test
  public void shouldGetTypeFromEmptyStruct() {
    // Given:
    final String schemaString = SqlTypes.struct().build().toString();

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlTypes.struct().build())));
  }

  @Test
  public void shouldGetTypeFromStructWithTwoFields() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR, B INT>";

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type, is(new Type(SqlStruct.builder()
        .field("A", SqlTypes.STRING)
        .field("B", SqlTypes.INTEGER)
        .build())));
  }

  @Test
  public void shouldReturnCustomTypeOnUnknownTypeName() {
    // Given:
    final String schemaString = "SHAKESPEARE";
    when(typeRegistry.resolveType(schemaString)).thenReturn(Optional.of(SqlTypes.STRING));

    // When:
    final Type type = parser.parse(schemaString);

    // Then:
    assertThat(type.getSqlType(), is(SqlTypes.STRING));
  }

  @Test
  public void shouldThrowOnNonIntegerPrecision() {
    // Given:
    final String schemaString = "DECIMAL(.1, 1)";

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse(schemaString)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Value must be integer for command: DECIMAL(PRECISION)"
    ));
  }

  @Test
  public void shouldThrowOnNonIntegerScale() {
    // Given:
    final String schemaString = "DECIMAL(1, 1.1)";

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse(schemaString)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Value must be integer for command: DECIMAL(SCALE)"
    ));
  }

  @Test
  public void shouldThrowMeaningfulErrorOnBadStructDeclaration() {
    // Given:
    final String schemaString = "STRUCT<foo VARCHAR,>";

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> parser.parse(schemaString)
    );

    // Then:
    System.out.println(e.getMessage());
    assertThat(e.getUnloggedMessage(), is(
        "Failed to parse: STRUCT<foo VARCHAR,>\nStatement: STRUCT<foo VARCHAR,>"
    ));
    assertThat(e.getMessage(), is(
        "Failed to parse schema"
    ));
    assertThat(e.getCause().getMessage(), is(
        "line 1:20: Syntax error at line 1:20"
    ));
  }
}