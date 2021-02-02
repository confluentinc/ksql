package io.confluent.ksql.rest.entity;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SchemaInfoTest {

  @Test
  public void shouldCorrectlyFormatDecimalsWithPrecisionAndScale() {

    final SchemaInfo schemaInfo= new SchemaInfo(
            SqlBaseType.DECIMAL,
            null,
            null,
            ImmutableMap.of("precision", 10, "scale", 9)
    );
    assertThat(schemaInfo.toTypeString(), equalTo("DECIMAL(10, 9)"));
  }

  @Test
  public void shouldCorrectlyFormatDecimalsWithoutParameters() {
    final SchemaInfo schemaInfo= new SchemaInfo(
            SqlBaseType.DECIMAL,
            null,
            null
    );
    assertThat(schemaInfo.toTypeString(), equalTo("DECIMAL"));
  }
}