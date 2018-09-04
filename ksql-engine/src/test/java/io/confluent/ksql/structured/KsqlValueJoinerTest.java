package io.confluent.ksql.structured;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.GenericRow;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class KsqlValueJoinerTest {

  private Schema leftSchema;
  private Schema rightSchema;
  private GenericRow leftRow;
  private GenericRow rightRow;

  @Before
  public void setUp() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("col0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("col1", SchemaBuilder.OPTIONAL_STRING_SCHEMA);

    leftSchema = schemaBuilder.build();
    rightSchema = schemaBuilder.build();

    leftRow = new GenericRow(Arrays.asList(12L, "foobar"));
    rightRow = new GenericRow(Arrays.asList(20L, "baz"));
  }

  @Test
  public void shouldJoinValueBothNonNull() {
    final SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    final GenericRow joined = joiner.apply(leftRow, rightRow);
    final List<Object> expected = Arrays.asList(12L, "foobar", 20L, "baz");
    assertEquals(expected, joined.getColumns());
  }

  @Test
  public void shouldJoinValueRightEmpty() {
    final SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    final GenericRow joined = joiner.apply(leftRow, null);
    final List<Object> expected = Arrays.asList(12L, "foobar", null, null);
    assertEquals(expected, joined.getColumns());
  }

  @Test
  public void shouldJoinValueLeftEmpty() {
    final SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    final GenericRow joined = joiner.apply(null, rightRow);
    final List<Object> expected = Arrays.asList(null, null, 20L, "baz");
    assertEquals(expected, joined.getColumns());
  }
}
