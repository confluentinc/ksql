/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
