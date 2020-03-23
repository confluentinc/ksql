package io.confluent.ksql.schema.ksql.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.ksql.DataException;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import org.junit.Test;

public class SqlNullTest {

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlTypes.NULL, SqlNull.INSTANCE);
  }

  @Test
  public void shouldReturnBaseType() {
    assertThat(SqlTypes.NULL.baseType(), is(SqlBaseType.NULL));
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlTypes.NULL.toString(), is("NULL"));
  }

  @Test
  public void shouldThrowOnValidateIfNotNull() {
    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> SqlTypes.NULL.validateValue("not-null")
    );

    // Then:
    assertThat(e.getMessage(), is("Expected NULL, got not-null"));
  }

  @Test
  public void shouldNotThrowOnValidateIfNull() {
    SqlTypes.NULL.validateValue(null);
  }
}