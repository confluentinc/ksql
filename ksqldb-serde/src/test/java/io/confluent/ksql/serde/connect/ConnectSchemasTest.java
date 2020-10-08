package io.confluent.ksql.serde.connect;

import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

public class ConnectSchemasTest {

  @Test
  public void shouldConvertColumnsToStructSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("Vic"), DOUBLE)
        .valueColumn(ColumnName.of("Bob"), BIGINT)
        .build();

    // When:
    final ConnectSchema result = ConnectSchemas.columnsToConnectSchema(schema.value());

    // Then:
    assertThat(result.type(), is(Type.STRUCT));
    assertThat(result.fields(), contains(
        connectField("Vic", 0, Schema.OPTIONAL_FLOAT64_SCHEMA),
        connectField("Bob", 1, Schema.OPTIONAL_INT64_SCHEMA)
    ));
  }

  private static org.apache.kafka.connect.data.Field connectField(
      final String fieldName,
      final int index,
      final Schema schema
  ) {
    return new org.apache.kafka.connect.data.Field(fieldName, index, schema);
  }
}