package io.confluent.ksql.schema.ksql;

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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

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
    final String schemaStringWithNull = "VARCHAR NULL";
    final String schemaStringRequired = "VARCHAR NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.STRING)));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.STRING)));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.STRING.required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromDecimal() {
    // Given:
    final String schemaString = "DECIMAL(2, 1)";
    final String schemaStringWithNull = "DECIMAL(2, 1) NULL";
    final String schemaStringRequired = "DECIMAL(2, 1) NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.decimal(2, 1))));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.decimal(2, 1))));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.decimal(2, 1).required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromStringArray() {
    // Given:
    final String schemaString = "ARRAY<VARCHAR>";
    final String schemaStringWithNull = "ARRAY<VARCHAR> NULL";
    final String schemaStringRequired = "ARRAY<VARCHAR> NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.STRING))));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.array(SqlTypes.STRING))));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.array(SqlTypes.STRING).required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromIntArray() {
    // Given:
    final String schemaString = "ARRAY<INT>";
    final String schemaStringWithNull = "ARRAY<INT> NULL";
    final String schemaStringRequired = "ARRAY<INT> NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.array(SqlTypes.INTEGER))));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.array(SqlTypes.INTEGER))));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.array(SqlTypes.INTEGER).required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromMap() {
    // Given:
    final String schemaString = "MAP<BIGINT, INT>";
    final String schemaStringWithNull = "MAP<BIGINT, INT> NULL";
    final String schemaStringRequired = "MAP<BIGINT, INT NOT NULL> NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER.required()).required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromStruct() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR>";
    final String schemaStringWithNull = "STRUCT<A VARCHAR> NULL";
    final String schemaStringRequired = "STRUCT<A VARCHAR> NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.struct().field("A", SqlTypes.STRING).build())));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeWithNull, is(new Type(SqlTypes.struct().field("A", SqlTypes.STRING).build())));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.struct().field("A", SqlTypes.STRING).build().required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromEmptyStruct() {
    // Given:
    final String schemaString = SqlTypes.struct().build().toString();
    final String schemaStringRequired = SqlTypes.struct().build().required().toString();

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlTypes.struct().build())));
    assertThat(type.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlTypes.struct().build().required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
  }

  @Test
  public void shouldGetTypeFromStructWithTwoFields() {
    // Given:
    final String schemaString = "STRUCT<A VARCHAR, B INT>";
    final String schemaStringWithNull = "STRUCT<A VARCHAR, B INT> NULL";
    final String schemaStringRequired = "STRUCT<A VARCHAR NOT NULL, B INT> NOT NULL";

    // When:
    final Type type = parser.parse(schemaString);
    final Type typeWithNull = parser.parse(schemaStringWithNull);
    final Type typeRequired = parser.parse(schemaStringRequired);

    // Then:
    assertThat(type, is(new Type(SqlStruct.builder()
        .field("A", SqlTypes.STRING)
        .field("B", SqlTypes.INTEGER)
        .build())));
    assertThat(typeWithNull, is(new Type(SqlStruct.builder()
            .field("A", SqlTypes.STRING)
            .field("B", SqlTypes.INTEGER)
            .build())));
    assertThat(typeWithNull.getSqlType().isOptional(), is(true));
    assertThat(typeRequired, is(new Type(SqlStruct.builder()
            .field("A", SqlTypes.STRING.required())
            .field("B", SqlTypes.INTEGER)
            .build().required())));
    assertThat(typeRequired.getSqlType().isOptional(), is(false));
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
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse(schemaString)
    );

    // Then:
    System.out.println(e.getMessage());
    assertThat(e.getMessage(), containsString(
        "Failed to parse: STRUCT<foo VARCHAR,>"
    ));
    assertThat(e.getCause().getMessage(), containsString(
        "line 1:20: mismatched input '>'"
    ));
  }
}