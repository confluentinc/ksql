package io.confluent.ksql.execution.streams;

import static io.confluent.ksql.schema.ksql.ColumnMatchers.keyColumn;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JoinParamsFactoryTest {

  private static final SourceName LEFT = SourceName.of("LEFT");
  private static final SourceName RIGHT = SourceName.of("RIGHT");

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.INTEGER)
      .build()
      .withAlias(LEFT)
      .withMetaAndKeyColsInValue();

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
      .build()
      .withAlias(RIGHT)
      .withMetaAndKeyColsInValue();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private JoinParams joinParams;

  @Test
  public void shouldBuildCorrectSchema() {
    // when:
    joinParams = JoinParamsFactory.create(LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(LEFT, SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(LEFT, SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(LEFT, ColumnName.of("BLUE"), SqlTypes.STRING)
        .valueColumn(LEFT, ColumnName.of("GREEN"), SqlTypes.INTEGER)
        .valueColumn(RIGHT, SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(RIGHT, SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(RIGHT, ColumnName.of("RED"), SqlTypes.BIGINT)
        .valueColumn(RIGHT, ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
        .build())
    );
  }

  @Test
  public void shouldThrowOnKeyTypeMismatch() {
    // Given:
    final LogicalSchema intKeySchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("BOB"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("GREEN"), SqlTypes.INTEGER)
        .build()
        .withAlias(LEFT)
        .withMetaAndKeyColsInValue();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid join. Key types differ: INTEGER vs STRING");

    // When:
    JoinParamsFactory.create(intKeySchema, RIGHT_SCHEMA);
  }

  @Test
  public void shouldGetKeyFromLeftSource() {
    // Given:
    final LogicalSchema leftSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("BOB"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
        .build()
        .withAlias(LEFT)
        .withMetaAndKeyColsInValue();

    final LogicalSchema rightSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("VIC"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("GREEN"), SqlTypes.DOUBLE)
        .build()
        .withAlias(RIGHT)
        .withMetaAndKeyColsInValue();

    // when:
    joinParams = JoinParamsFactory.create(leftSchema, rightSchema);

    // Then:
    assertThat(joinParams.getSchema().key(), contains(
        keyColumn(ColumnName.of("BOB"), SqlTypes.BIGINT)
    ));
  }
}