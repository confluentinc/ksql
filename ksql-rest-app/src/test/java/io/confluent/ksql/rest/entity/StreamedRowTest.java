package io.confluent.ksql.rest.entity;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class StreamedRowTest {
  @Test
  public void shouldReturnRowStringForRow() {
    // When:
    final GenericRow genericRow = new GenericRow(1, 2, 3);
    final StreamedRow row = StreamedRow.row(genericRow);

    // Then:
    assertThat(row.toString(), equalTo(genericRow.toString()));
  }

  @Test
  public void shouldReturnErrorStringForErrorRow() {
    // When:
    final KsqlException exception = new KsqlException("badbadbad");
    final StreamedRow row = StreamedRow.error(exception);

    // Then:
    assertThat(row.toString(), equalTo(row.getErrorMessage().toString()));
  }

  @Test
  public void shouldReturnFinalMessageStringForFinalRow() {
    // When:
    final StreamedRow row = StreamedRow.finalMessage("foo");

    // Then:
    assertThat(row.toString(), equalTo(row.getFinalMessage()));
  }
}