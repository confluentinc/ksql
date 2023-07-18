package io.confluent.ksql.rest.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.vertx.core.buffer.Buffer;
import java.math.BigDecimal;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlTargetUtilTest {

  @Mock
  private KsqlEntityList entities;

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
    assertThat(row.getHeader().get().getSchema().key(), is(Collections.emptyList()));
    assertThat(row.getHeader().get().getSchema().value().size(), is(2));
    assertThat(row.getHeader().get().getSchema().value().get(0),
        is (Column.of(ColumnName.of("col1"), SqlTypes.BIGINT, Namespace.VALUE, 0)));
    assertThat(row.getHeader().get().getSchema().value().get(1),
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
}
