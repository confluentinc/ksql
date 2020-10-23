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

import static com.google.common.collect.ImmutableList.of;
import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.rest.entity.StreamedRow.header;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

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
import org.junit.Test;

public class RestTestExecutorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("col0"), SqlTypes.STRING)
      .build();

  @Test
  public void shouldFailToVerifyOnDifferentColumnsSchema() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(of(
        header(new QueryId("not checked"), SCHEMA)
    ));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> response.verify(
            "query",
            of(
                ImmutableMap.of("header",
                    ImmutableMap.of(
                        "schema", "`expected` STRING"
                    )
                )
            ),
            of("the SQL"),
            0
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Response mismatch at responses[0]->query[0]->header->schema"));
    assertThat(e.getMessage(), containsString(
        "Expected: is \"`expected` STRING\""));
    assertThat(e.getMessage(), containsString(
        "but: was \"`col0` STRING\""));
  }

  @Test
  public void shouldFailToVerifyOnDifferentRowValues() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(of(
        StreamedRow.pushRow(genericRow("key", 55, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> response.verify(
            "query",
            of(
                ImmutableMap.of("row",
                    ImmutableMap.of("columns", of("key", 1, new BigDecimal("66.675")))
                )
            ),
            of("the SQL"),
            0
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Response mismatch at responses[0]->query[0]->row->columns"));
    assertThat(e.getMessage(), containsString(
        "Expected: is <[key, 1, 66.675]>"));
    assertThat(e.getMessage(), containsString(
        "but: was <[key, 55, 66.675]>"));
  }

  @Test
  public void shouldFailToVerifyOnDifferentTombstoneValue() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(of(
        StreamedRow.pushRow(genericRow("key", 55, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> response.verify(
            "query",
            of(
                ImmutableMap.of("row",
                    ImmutableMap.of(
                        "columns", of("key", 55, new BigDecimal("66.675")),
                        "tombstone", true
                    )
                )
            ),
            of("the SQL"),
            0
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Response mismatch at responses[0]->query[0]->row"));
    assertThat(e.getMessage(), containsString(
        "Expected: map containing [\"tombstone\"->ANYTHING]"));
    assertThat(e.getMessage(), containsString(
        "but: map was [<columns=[key, 55, 66.675]>]"));
  }

  @Test
  public void shouldPassVerificationOnMatch() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
        header(new QueryId("not checked"), SCHEMA),
        StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675")))
    ));

    // When:
    response.verify(
        "query",
        ImmutableList.of(
            ImmutableMap.of("header",
                ImmutableMap.of(
                    "schema", "`col0` STRING"
                )
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