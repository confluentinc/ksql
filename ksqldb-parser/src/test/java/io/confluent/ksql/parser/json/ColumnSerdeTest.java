package io.confluent.ksql.parser.json;

import static io.confluent.ksql.parser.json.ColumnTestCase.COLUMN;
import static io.confluent.ksql.parser.json.ColumnTestCase.COLUMN_STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import org.junit.Test;

public class ColumnSerdeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldSerializeColumn() throws IOException {
    assertThat(MAPPER.writeValueAsString(COLUMN), equalTo(COLUMN_STRING));
  }

  @Test
  public void shouldDeserializeColumnString() throws IOException {
    final Column column = MAPPER.readValue(COLUMN_STRING, Column.class);
    assertThat(column.name(), equalTo(COLUMN.name()));
    assertThat(column.type(), equalTo(COLUMN.type()));
    assertThat(column.namespace(), equalTo(COLUMN.namespace()));
  }
}
