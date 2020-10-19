/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.Test;

public class StreamedRowTest {

  static {
    // Force KsqlClient class to initialise so LogicalSchema deserializers are correctly installed.
    KsqlClient.initialize();
  }

  private static final ObjectMapper MAPPER = ApiJsonMapper.INSTANCE.get();
  private static final QueryId QUERY_ID = new QueryId("theQueryId");
  private static final KsqlHostInfoEntity hostInfo = new KsqlHostInfoEntity("host", 80);

  // Pull query schemas have both key and value columns:
  private static final LogicalSchema PULL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ID"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("VAL"), SqlTypes.STRING)
      .build();

  @Test
  public void shouldRoundTripPullHeader() throws Exception {
    final StreamedRow row = StreamedRow.pullHeader(QUERY_ID, PULL_SCHEMA);

    final String expectedJson = "{\"header\":{"
        + "\"queryId\":\"theQueryId\","
        + "\"schema\":\"`ID` BIGINT KEY, `VAL` STRING\""
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripPushHeader() throws Exception {
    final StreamedRow row = StreamedRow.pushHeader(
        QUERY_ID,
        ImmutableList.of(
            Column.of(ColumnName.of("ID"), SqlTypes.BIGINT, Namespace.KEY, 0)
        ),
        ImmutableList.of(
            Column.of(ColumnName.of("VAL"), SqlTypes.STRING, Namespace.VALUE, 0)
        )
    );

    final String expectedJson = "{\"header\":{"
        + "\"queryId\":\"theQueryId\","
        + "\"key\":\"`ID` BIGINT\","
        + "\"schema\":\"`VAL` STRING\""
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripTableRow() throws Exception {
    final StreamedRow row = StreamedRow.tableRow(
        ImmutableList.<Object>of("some", 123456789123456789L),
        GenericRow.genericRow("v0", new BigDecimal("1.2"), 4)
    );

    final String expectedJson = "{\"row\":{"
        + "\"key\":[\"some\",123456789123456789],"
        + "\"columns\":[\"v0\",1.2,4]"
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripStreamRow() throws Exception {
    final StreamedRow row = StreamedRow.streamRow(
        GenericRow.genericRow("v0", new BigDecimal("1.2"), 4)
    );

    final String expectedJson = "{\"row\":{"
        + "\"columns\":[\"v0\",1.2,4]"
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripPullRow() throws Exception {
    final StreamedRow row = StreamedRow.pullRow(
        GenericRow.genericRow("v0", new BigDecimal("1.2"), 4),
        Optional.of(hostInfo)
    );

    final String expectedJson = "{\"row\":{"
        + "\"columns\":[\"v0\",1.2,4]},"
        + "\"sourceHost\":\"host:80\""
        + "}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripPullRowNoHost() throws Exception {
    final StreamedRow row = StreamedRow.pullRow(
        GenericRow.genericRow("v0", new BigDecimal("1.2"), 4),
        Optional.empty()
    );

    final String expectedJson = "{\"row\":{"
        + "\"columns\":[\"v0\",1.2,4]"
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripTableTombstone() throws Exception {
    final StreamedRow row = StreamedRow.tombstone(
        ImmutableList.<Object>of("some", 123456789123456789L)
    );

    final String expectedJson = "{\"row\":{"
        + "\"key\":[\"some\",123456789123456789],"
        + "\"tombstone\":true"
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripGenericError() throws Exception {
    final StreamedRow row = StreamedRow.error(
        new RuntimeException("Boom"),
        1234
    );

    final String expectedJson = "{\"errorMessage\":{"
        + "\"@type\":\"generic_error\","
        + "\"error_code\":1234,"
        + "\"message\":\"Boom\""
        + "}}";

    testRoundTrip(row, expectedJson);
  }

  @Test
  public void shouldRoundTripFinalMessage() throws Exception {
    final StreamedRow row = StreamedRow.finalMessage(
        "Hooray!"
    );

    final String expectedJson = "{\"finalMessage\":\"Hooray!\"}";

    testRoundTrip(row, expectedJson);
  }


  private static void testRoundTrip(
      final StreamedRow row,
      final String expectedJson
  ) throws Exception {
    // When:
    final String json = MAPPER.writeValueAsString(row);

    // Then:
    assertThat(json, is(expectedJson));

    // When:
    final StreamedRow result = MAPPER.readValue(json, StreamedRow.class);

    // Then:
    assertThat(result, is(row));
  }
}