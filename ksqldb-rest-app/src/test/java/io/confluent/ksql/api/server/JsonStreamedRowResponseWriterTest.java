/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonStreamedRowResponseWriterTest {

  private static final String QUERY_ID = "queryId";
  private static final List<String> COL_NAMES = ImmutableList.of(
      "A", "B", "C"
  );
  private static final List<String> COL_TYPES = ImmutableList.of(
      "INTEGER", "DOUBLE", "ARRAY"
  );
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("A"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("B"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("C"), SqlTypes.array(SqlTypes.STRING))
      .build();

  @Mock
  private HttpServerResponse response;
  @Mock
  private QueryPublisher publisher;

  private JsonStreamedRowResponseWriter writer;
  private StringBuilder stringBuilder = new StringBuilder();

  @Before
  public void setUp() {
    when(response.write((Buffer) any())).thenAnswer(a -> {
      final Buffer buffer = a.getArgument(0);
      stringBuilder.append(new String(buffer.getBytes(), StandardCharsets.UTF_8));
      return response;
    });
    when(response.write((String) any())).thenAnswer(a -> {
      final String str = a.getArgument(0);
      stringBuilder.append(str);
      return response;
    });

    writer = new JsonStreamedRowResponseWriter(response, publisher, Optional.empty(),
        Optional.empty());
  }

  private void setupWithMessages(String completionMessage, String limitMessage) {
    writer = new JsonStreamedRowResponseWriter(response, publisher, Optional.of(completionMessage),
        Optional.of(limitMessage));
  }

  @Test
  public void shouldSucceedWithNoEndMessages() {
    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\"," 
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}}]"));
  }

  @Test
  public void shouldSucceedWithCompletionMessage() {
    // Given:
    setupWithMessages("complete!", "limit hit!");

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"finalMessage\":\"complete!\"}]"));
  }

  @Test
  public void shouldSucceedWithLimitMessage() {
    // Given:
    setupWithMessages("complete!", "limit hit!");

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
        KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeLimitMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
        is("[{\"header\":{\"queryId\":\"queryId\","
            + "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"}},\n"
            + "{\"row\":{\"columns\":[123,234.0,[\"hello\"]]}},\n"
            + "{\"finalMessage\":\"limit hit!\"}]"));
  }
}
