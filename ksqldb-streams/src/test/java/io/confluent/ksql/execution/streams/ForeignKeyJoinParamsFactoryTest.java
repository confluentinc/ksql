package io.confluent.ksql.execution.streams;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Iterables;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class ForeignKeyJoinParamsFactoryTest {

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_FOREIGN_KEY"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
      .build();

  @Test
  public void shouldBuildCorrectKeyedSchema() {
    // Given:
    final ColumnName leftJoinColumnName = ColumnName.of("L_FOREIGN_KEY");

    // When:
    final ForeignKeyJoinParams<String> joinParams =
        ForeignKeyJoinParamsFactory.create(leftJoinColumnName, LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_FOREIGN_KEY"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
        .build())
    );
  }

  @Test
  public void shouldThrowIfJoinColumnNotFound() {
    // Given:
    final ColumnName leftJoinColumnName = ColumnName.of("L_UNKNOWN");

    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> ForeignKeyJoinParamsFactory.create(leftJoinColumnName, LEFT_SCHEMA, RIGHT_SCHEMA)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Could not find join column in left input table.")
    );
  }
}