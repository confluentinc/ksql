/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class KsqlValueJoinerTest {

  private LogicalSchema leftSchema;
  private LogicalSchema rightSchema;
  private GenericRow leftRow;
  private GenericRow rightRow;

  @Before
  public void setUp() {
    leftSchema = LogicalSchema.builder()
        .valueField("col0", SqlTypes.BIGINT)
        .valueField("col1", SqlTypes.STRING)
        .build();

    rightSchema = leftSchema;

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
