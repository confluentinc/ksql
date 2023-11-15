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
import io.confluent.ksql.api.server.JsonStreamedRowResponseWriter.RowFormat;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.test.rest.RestTestExecutor.RqttQueryProtoResponse;
import io.confluent.ksql.test.rest.RestTestExecutor.RqttQueryResponse;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
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
            0,
            false
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Response mismatch"));
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
            0,
            false
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
            0,
            false
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
        0,
        false
    );
  }

  @Test
  public void shouldPassVerificationOnMatchMultipleRows() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 57, new BigDecimal("66.675")))
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
        ),
        ImmutableMap.of("row",
          ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
        ),
        ImmutableMap.of("row",
          ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("66.675")))
        )
      ),
      ImmutableList.of("the SQL"),
      0,
      false
    );
  }

  @Test
  public void shouldPassVerificationOnMatchMultipleRowsReOrdered() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 57, new BigDecimal("66.675")))
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
          ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
        ),
        ImmutableMap.of("row",
          ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("66.675")))
        ),
        ImmutableMap.of("row",
          ImmutableMap.of("columns", ImmutableList.of("key", 55, new BigDecimal("66.675")))
        )
      ),
      ImmutableList.of("the SQL"),
      0,
      false
    );
  }

  @Test
  public void shouldFailVerificationOnUnorderedWithVerifyOrder() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 57, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
      AssertionError.class,
      () -> response.verify(
        "query",
        ImmutableList.of(
          ImmutableMap.of("header",
            ImmutableMap.of(
              "schema", "`col0` STRING"
            )
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 55, new BigDecimal("66.675")))
          )
        ),
        ImmutableList.of("the SQL"),
        0,
        true
      )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Response mismatch"));
  }

  @Test
  public void shouldFailVerificationRowCountMismatch() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 57, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
      AssertionError.class,
      () -> response.verify(
        "query",
        ImmutableList.of(
          ImmutableMap.of("header",
            ImmutableMap.of(
              "schema", "`col0` STRING"
            )
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("66.675")))
          )
        ),
        ImmutableList.of("the SQL"),
        0,
        false
      )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "row count mismatch"));
  }

  @Test
  public void shouldFailVerificationOnMatchWrongRow() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 57, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
      AssertionError.class,
      () -> response.verify(
        "query",
        ImmutableList.of(
          ImmutableMap.of("header",
            ImmutableMap.of(
              "schema", "`col0` STRING"
            )
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 58, new BigDecimal("66.675")))
          )
        ),
        ImmutableList.of("the SQL"),
        0,
        false
      )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Response mismatch"));
  }

  @Test
  public void shouldFailVerificationOnMatchDuplicateRow() {
    // Given:
    final RqttQueryResponse response = new RqttQueryResponse(ImmutableList.of(
      header(new QueryId("not checked"), SCHEMA),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 56, new BigDecimal("66.675"))),
      StreamedRow.pushRow(GenericRow.genericRow("key", 55, new BigDecimal("66.675")))
    ));

    // When:
    final AssertionError e = assertThrows(
      AssertionError.class,
      () -> response.verify(
        "query",
        ImmutableList.of(
          ImmutableMap.of("header",
            ImmutableMap.of(
              "schema", "`col0` STRING"
            )
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 55, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 56, new BigDecimal("66.675")))
          ),
          ImmutableMap.of("row",
            ImmutableMap.of("columns", ImmutableList.of("key", 57, new BigDecimal("76.675")))
          )
        ),
        ImmutableList.of("the SQL"),
        0,
        false
      )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Uneven occurrence of expected vs actual for row [key, 55, 66.675]"));
  }

  @Test
  public void shouldVerifyUnorderedProtoResponse() {
    // Given:
    final RqttQueryProtoResponse response = new RqttQueryProtoResponse(ImmutableList.of(
        RowFormat.PROTOBUF.metadataRow(new QueryResponseMetadata(
            "not checked",
            ImmutableList.of("col0"),
            ImmutableList.of("STRING"),
            SCHEMA)
        ),
        RowFormat.PROTOBUF.dataRow(new KeyValueMetadata<>(KeyValue.keyValue(null, GenericRow.genericRow("hello")))),
        RowFormat.PROTOBUF.dataRow(new KeyValueMetadata<>(KeyValue.keyValue(null, GenericRow.genericRow("goodbye")))),
        StreamedRow.finalMessage("end")
    ));

    // When:
    response.verify(
        "queryProto",
        ImmutableList.of(
            ImmutableMap.of("header", ImmutableMap.of("protoSchema", "syntax = \"proto3\";\n\nmessage ConnectDefault1 {\n  string col0 = 1;\n}\n")),
            ImmutableMap.of("finalMessage", "end"),
            ImmutableMap.of("row", "col0: \"goodbye\"\n"),
            ImmutableMap.of("row", "col0: \"hello\"\n")
        ),
        null,
        0,
        false
    );
  }

  @Test
  public void shouldThrowOnMisorderedProtoResponse() {
    // Given:
    final RqttQueryProtoResponse response = new RqttQueryProtoResponse(ImmutableList.of(
        RowFormat.PROTOBUF.metadataRow(new QueryResponseMetadata(
            "not checked",
            ImmutableList.of("col0"),
            ImmutableList.of("STRING"),
            SCHEMA)
        ),
        RowFormat.PROTOBUF.dataRow(new KeyValueMetadata<>(KeyValue.keyValue(null, GenericRow.genericRow("hello")))),
        RowFormat.PROTOBUF.dataRow(new KeyValueMetadata<>(KeyValue.keyValue(null, GenericRow.genericRow("goodbye")))),
        StreamedRow.finalMessage("end")
    ));

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> response.verify(
            "queryProto",
            ImmutableList.of(
                ImmutableMap.of("header", ImmutableMap.of("protoSchema", "syntax = \"proto3\";\n\nmessage ConnectDefault1 {\n  string col0 = 1;\n}\n")),
                ImmutableMap.of("row", "col0: \"goodbye\"\n"),
                ImmutableMap.of("row", "col0: \"hello\"\n"),
                ImmutableMap.of("finalMessage", "end")
            ),
            null,
            0,
            true
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Response mismatch"));
  }
}

