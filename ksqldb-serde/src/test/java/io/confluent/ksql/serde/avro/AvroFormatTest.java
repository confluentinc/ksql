package io.confluent.ksql.serde.avro;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroFormatTest {

  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srFactory;
  @Mock
  private FormatInfo formatInfo;

  private AvroFormat format;
  private Map<String, String> formatProps;

  @Before
  public void setUp() {
    format = new AvroFormat();
    formatProps = new HashMap<>();

    when(formatInfo.getProperties()).thenReturn(formatProps);
  }

  @Test
  public void shouldThrowWhenCreatingSerdeIfSchemaContainsInvalidAvroNames() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema.from(
        ImmutableList.of(column("1AintRight")),
        EnabledSerdeFeatures.of()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> format.getSerde(schema, formatProps, config, srFactory)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema is not compatible with Avro: Illegal initial character: 1AintRight"));
  }

  @Test
  public void shouldThrowWhenBuildingAvroSchemafSchemaContainsInvalidAvroNames() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema.from(
        ImmutableList.of(column("2Bad")),
        EnabledSerdeFeatures.of()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> format.toParsedSchema(schema, formatInfo)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema is not compatible with Avro: Illegal initial character: 2Bad"));
  }

  private static SimpleColumn column(final String name) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(SqlTypes.BIGINT);
    return column;
  }
}