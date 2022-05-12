package io.confluent.ksql.rest.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.vertx.core.buffer.Buffer;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlTargetUtilTest {

  @Mock
  private KsqlEntityList entities;

  @BeforeClass
  public static void setupClass() {
    // Have to know how to deserialize types!
    ApiJsonMapper.INSTANCE.get().registerModule(new KsqlTypesDeserializationModule());
  }

  @Before
  public void setUp() {
  }

  @Test
  public void shouldParseHeader() {
    // When:
    final StreamedRow row = KsqlTargetUtil.toRowFromDelimited(Buffer.buffer(
        "{\"queryId\": \"query_id_10\", "
            + "\"columnNames\":[\"col1\",\"col2\"], "
            + "\"columnTypes\":[\"BIGINT\",\"DOUBLE\"]}"));

    // Then:
    assertThat(row.getHeader().isPresent(), is(true));
    assertThat(row.getHeader().get().getQueryId().toString(), is("query_id_10"));
    assertThat(row.getHeader().get().getSchema().get().key(), is(Collections.emptyList()));
    assertThat(row.getHeader().get().getSchema().get().value().size(), is(2));
    assertThat(row.getHeader().get().getSchema().get().value().get(0),
        is (Column.of(ColumnName.of("col1"), SqlTypes.BIGINT, Namespace.VALUE, 0)));
    assertThat(row.getHeader().get().getSchema().get().value().get(1),
        is (Column.of(ColumnName.of("col2"), SqlTypes.DOUBLE, Namespace.VALUE, 1)));
  }

  @Test
  public void shouldParseError() {
    // When:
    final StreamedRow row = KsqlTargetUtil.toRowFromDelimited(Buffer.buffer(
        "{\"@type\":\"generic_error\",\"error_code\": 500,\"message\":\"Really bad problem\"}"));

    // Then:
    assertThat(row.getErrorMessage().isPresent(), is(true));
    assertThat(row.getErrorMessage().get().getErrorCode(), is(500));
    assertThat(row.getErrorMessage().get().getMessage(), is("Really bad problem"));
  }

  @Test
  public void shouldParseRow() {
    // When:
    final StreamedRow row = KsqlTargetUtil.toRowFromDelimited(Buffer.buffer(
        "[3467362496,5.5]"));

    // Then:
    assertThat(row.getRow().isPresent(), is(true));
    assertThat(row.getRow().get().getColumns().size(), is(2));
    assertThat(row.getRow().get().getColumns().get(0), is(3467362496L));
    assertThat(row.getRow().get().getColumns().get(1), is(BigDecimal.valueOf(5.5d)));
  }

  @Test
  public void shouldError_badJSON() {
    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> KsqlTargetUtil.toRowFromDelimited(Buffer.buffer("[34"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Couldn't parse message: [34"));
  }

  @Test
  public void toRows() {
    // When:
    final List<StreamedRow> rows =  KsqlTargetUtil.toRows(Buffer.buffer(
        "[{\"header\":{\"queryId\":\"query_id_10\",\"schema\":\"`col1` STRING\"}},\n"
        + "{\"row\":{\"columns\":[\"Row1\"]}},\n"
        + "{\"row\":{\"columns\":[\"Row2\"]}},\n"));

    // Then:
    assertThat(rows.size(), is(3));
    final StreamedRow row = rows.get(0);
    assertThat(row.getHeader().isPresent(), is(true));
    assertThat(row.getHeader().get().getQueryId().toString(), is("query_id_10"));
    assertThat(row.getHeader().get().getSchema().get().key(), is(Collections.emptyList()));
    assertThat(row.getHeader().get().getSchema().get().value().size(), is(1));
    assertThat(row.getHeader().get().getSchema().get().value().get(0),
        is (Column.of(ColumnName.of("col1"), SqlTypes.STRING, Namespace.VALUE, 0)));
    final StreamedRow row2 = rows.get(1);
    assertThat(row2.getRow().isPresent(), is(true));
    assertThat(row2.getRow().get().getColumns(), is(ImmutableList.of("Row1")));
    final StreamedRow row3 = rows.get(2);
    assertThat(row3.getRow().isPresent(), is(true));
    assertThat(row3.getRow().get().getColumns(), is(ImmutableList.of("Row2")));

  }

  @Test
  public void toRows_errorParsingNotAtEnd() {
    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        () -> KsqlTargetUtil.toRows(Buffer.buffer(
            "[{\"header\":{\"queryId\":\"query_id_10\",\"schema\":\"`col1` STRING\"}},\n"
                + "{\"row\":{\"columns\"\n"
                + "{\"row\":{\"columns\":[\"Row2\"]}},\n"))
    );

    // Then:
    assertThat(e.getMessage(), is(("Failed to deserialise object")));
  }

  @Test
  public void shouldParseHeaderProto() {
    // When:
    final List<StreamedRow> rows = KsqlTargetUtil.toRowsFromProto(Buffer.buffer(
            "[{\"header\":{" +
                    "\"queryId\":\"query_1652327621826\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n\\nmessage ConnectDefault1 {\\n  int64 COUNT = 1;\\n  string USERID = 2;\\n}\\n\"}}"));

    StreamedRow row = rows.get(0);

    // Then:
    assertThat(row.getHeader().isPresent(), is(true));
    assertThat(row.getHeader().get().getQueryId().toString(), is("query_1652327621826"));

    assertThat(row.getHeader().get().getSchema(), is(Optional.empty()));
    assertThat(row.getHeader().get().getProtoSchema().get(), is("syntax = \"proto3\";\n\nmessage ConnectDefault1 {\n  int64 COUNT = 1;\n  string USERID = 2;\n}\n"));
  }

  @Test
  public void shouldError_badJSONProto() {
    // When:
    final Exception e = assertThrows(
            KsqlRestClientException.class,
            () -> KsqlTargetUtil.toRowsFromProto(Buffer.buffer("[34"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to deserialise object"));
  }

  @Test
  public void toRowsProto() {
    // When:
    final List<StreamedRow> rows =  KsqlTargetUtil.toRowsFromProto(Buffer.buffer(
            "[{\"header\":{" +
                    "\"queryId\":\"query_1652329281296\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n\\nmessage ConnectDefault1 {\\n  int64 COUNT = 1;\\n  string USERID = 2;\\n}\\n\"}}," +
                    "{\"row\":\"CAESBlVTRVJfMA==\"}," +
                    "{\"row\":\"CAESBlVTRVJfMQ==\"}," +
                    "{\"row\":\"CAISBlVTRVJfMg==\"}," +
                    "{\"row\":\"CAISBlVTRVJfMw==\"}," +
                    "{\"row\":\"CAESBlVTRVJfNA==\"}]"));

    // Then:
    assertThat(rows.size(), is(6));
    final StreamedRow row = rows.get(0);
    assertThat(row.getHeader().isPresent(), is(true));
    assertThat(row.getHeader().get().getQueryId().toString(), is("query_1652329281296"));
    assertThat(row.getHeader().get().getSchema(), is(Optional.empty()));
    assertThat(row.getHeader().get().getProtoSchema().get(), is("syntax = \"proto3\";\n\nmessage ConnectDefault1 {\n  int64 COUNT = 1;\n  string USERID = 2;\n}\n"));

    final StreamedRow row2 = rows.get(1);
    assertThat(row2.getRow().isPresent(), is(false));
    assertThat(row2.getRowProtobuf().isPresent(), is(true));
    assertThat(row2.getRowProtobuf().get().toString(), is("{\"row\":\"CAESBlVTRVJfMA==\"}"));

    final StreamedRow row3 = rows.get(2);
    assertThat(row3.getRow().isPresent(), is(false));
    assertThat(row3.getRowProtobuf().isPresent(), is(true));
    assertThat(row3.getRowProtobuf().get().toString(), is("{\"row\":\"CAESBlVTRVJfMQ==\"}"));

    final StreamedRow row4 = rows.get(3);
    assertThat(row4.getRow().isPresent(), is(false));
    assertThat(row4.getRowProtobuf().isPresent(), is(true));
    assertThat(row4.getRowProtobuf().get().toString(), is("{\"row\":\"CAISBlVTRVJfMg==\"}"));

    final StreamedRow row5 = rows.get(4);
    assertThat(row5.getRow().isPresent(), is(false));
    assertThat(row5.getRowProtobuf().isPresent(), is(true));
    assertThat(row5.getRowProtobuf().get().toString(), is("{\"row\":\"CAISBlVTRVJfMw==\"}"));

    final StreamedRow row6 = rows.get(5);
    assertThat(row6.getRow().isPresent(), is(false));
    assertThat(row6.getRowProtobuf().isPresent(), is(true));
    assertThat(row6.getRowProtobuf().get().toString(), is("{\"row\":\"CAESBlVTRVJfNA==\"}"));
  }

  @Test
  public void toRows_errorParsingNotAtEndProto() {
    // When:
    final Exception e = assertThrows(
            KsqlRestClientException.class,
            () -> KsqlTargetUtil.toRowsFromProto(Buffer.buffer(
                    "[{\"header\":{" +
                            "\"queryId\":\"query_1652329281296\"," +
                            "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n\\nmessage ConnectDefault1 {\\n  int64 COUNT = 1;\\n  string USERID = 2;\\n}\\n\"}}," +
                            "{\"row\":\"CAESBlVTRVJfMA==\"}," +
                            "{\"row\":\"CAESB"))
    );

    // Then:
    assertThat(e.getMessage(), is(("Failed to deserialise object")));
  }
}
