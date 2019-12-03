package io.confluent.ksql.execution.streams;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import org.junit.Test;

public class JoinParamsFactoryTest {

  private static final SourceName LEFT = SourceName.of("LEFT");
  private static final SourceName RIGHT = SourceName.of("RIGHT");

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.INTEGER)
      .build()
      .withAlias(LEFT);

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
      .build()
      .withAlias(RIGHT);

  @Test
  public void shouldBuildCorrectSchema() {
    final LogicalSchema expected = LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(LEFT, SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(LEFT, SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(LEFT, ColumnName.of("BLUE"), SqlTypes.STRING)
        .valueColumn(LEFT, ColumnName.of("GREEN"), SqlTypes.INTEGER)
        .valueColumn(RIGHT, SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(RIGHT, SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(RIGHT, ColumnName.of("RED"), SqlTypes.BIGINT)
        .valueColumn(RIGHT, ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
        .build();

    assertThat(JoinParamsFactory.createSchema(LEFT_SCHEMA, RIGHT_SCHEMA), is(expected));
  }
}