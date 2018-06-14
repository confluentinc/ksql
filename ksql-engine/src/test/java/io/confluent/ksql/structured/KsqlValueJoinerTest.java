package io.confluent.ksql.structured;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import io.confluent.ksql.GenericRow;

import static org.junit.Assert.assertEquals;

public class KsqlValueJoinerTest {

  private Schema leftSchema;
  private Schema rightSchema;
  private GenericRow leftRow;
  private GenericRow rightRow;

  @Before
  public void setUp() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("col0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("col1", SchemaBuilder.OPTIONAL_STRING_SCHEMA);

    leftSchema = schemaBuilder.build();
    rightSchema = schemaBuilder.build();

    leftRow = new GenericRow(Arrays.asList(12L, "foobar"));
    rightRow = new GenericRow(Arrays.asList(20L, "baz"));
  }

  @Test
  public void shouldJoinValueBothNonNull() {
    SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    GenericRow joined = joiner.apply(leftRow, rightRow);
    List<Object> expected = Arrays.asList(12L, "foobar", 20L, "baz");
    assertEquals(expected, joined.getColumns());
  }

  @Test
  public void shouldJoinValueRightEmpty() {
    SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    GenericRow joined = joiner.apply(leftRow, null);
    List<Object> expected = Arrays.asList(12L, "foobar", null, null);
    assertEquals(expected, joined.getColumns());
  }

  @Test
  public void shouldJoinValueLeftEmpty() {
    SchemaKStream.KsqlValueJoiner joiner = new SchemaKStream.KsqlValueJoiner(leftSchema,
                                                                             rightSchema);

    GenericRow joined = joiner.apply(null, rightRow);
    List<Object> expected = Arrays.asList(null, null, 20L, "baz");
    assertEquals(expected, joined.getColumns());
  }
}
