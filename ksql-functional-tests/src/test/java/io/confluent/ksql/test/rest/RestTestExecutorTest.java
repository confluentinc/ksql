/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.test.rest.RestTestExecutor.RqttQueryResponse;
import java.math.BigDecimal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RestTestExecutorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("col0"), SqlTypes.STRING)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailToVerifyOnDifferentSchema() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
        StreamedRow.header(new QueryId("not checked"), SCHEMA)
    ));

    // Expect:
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage("Response mismatch at responses[0]->query[0]->header->schema");
    expectedException.expectMessage("Expected: is \"`ROWKEY` STRING KEY, `expected` STRING\"");
    expectedException.expectMessage("but: was \"`ROWKEY` STRING KEY, `col0` STRING\"");

    // When:
    response.verify(
        "query",
        ImmutableList.of(
            ImmutableMap.of("header",
                ImmutableMap.of("schema", "`ROWKEY` STRING KEY, `expected` STRING")
            )
        ),
        ImmutableList.of("the SQL"),
        0
    );
  }

  @Test
  public void shouldFailToVerifyOnDifferentRowValues() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
        StreamedRow.row(new GenericRow("key", 55, new BigDecimal("66.675")))
    ));

    // Expect:
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage("Response mismatch at responses[0]->query[0]->row->columns");
    expectedException.expectMessage("Expected: is <[key, 1, 66.675]>");
    expectedException.expectMessage("but: was <[key, 55, 66.675]>");

    // When:
    response.verify(
        "query",
        ImmutableList.of(
            ImmutableMap.of("row",
                ImmutableMap.of("columns", ImmutableList.of("key", 1, new BigDecimal("66.675")))
            )
        ),
        ImmutableList.of("the SQL"),
        0
    );
  }

  @Test
  public void shouldPassVerificationOnMatch() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
        StreamedRow.header(new QueryId("not checked"), SCHEMA),
        StreamedRow.row(new GenericRow("key", 55, new BigDecimal("66.675")))
    ));

    // When:
    response.verify(
        "query",
        ImmutableList.of(
            ImmutableMap.of("header",
                ImmutableMap.of("schema", "`ROWKEY` STRING KEY, `col0` STRING")
            ),
            ImmutableMap.of("row",
                ImmutableMap.of("columns", ImmutableList.of("key", 55, new BigDecimal("66.675")))
            )
        ),
        ImmutableList.of("the SQL"),
        0
    );
  }
}