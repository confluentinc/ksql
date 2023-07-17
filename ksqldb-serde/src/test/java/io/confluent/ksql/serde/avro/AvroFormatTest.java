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
import io.confluent.ksql.serde.SerdeFeatures;
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

  private AvroFormat format;
  private Map<String, String> formatProps;

  @Before
  public void setUp() {
    format = new AvroFormat();
    formatProps = new HashMap<>();
  }

  @Test
  public void shouldThrowWhenCreatingSerdeIfSchemaContainsInvalidAvroNames() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema.from(
        ImmutableList.of(column("1AintRight")),
        SerdeFeatures.of()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> format.getSerde(schema, formatProps, config, srFactory, false)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema is not compatible with Avro: Illegal initial character: 1AintRight"));
  }

  @Test
  public void shouldConvertNames() {
    assertThat(AvroFormat.getKeySchemaName("foo"), is("io.confluent.ksql.avro_schemas.FooKey"));
    assertThat(AvroFormat.getKeySchemaName("Foo"), is("io.confluent.ksql.avro_schemas.FooKey"));
    assertThat(AvroFormat.getKeySchemaName("FOO"), is("io.confluent.ksql.avro_schemas.FooKey"));
    assertThat(AvroFormat.getKeySchemaName("FOO_BAR"), is("io.confluent.ksql.avro_schemas.FooBarKey"));
    assertThat(AvroFormat.getKeySchemaName("Foo_Bar"), is("io.confluent.ksql.avro_schemas.FooBarKey"));
    assertThat(AvroFormat.getKeySchemaName("foo_bar"), is("io.confluent.ksql.avro_schemas.FooBarKey"));
    assertThat(AvroFormat.getKeySchemaName("fOoBaR"), is("io.confluent.ksql.avro_schemas.FoobarKey"));
    assertThat(AvroFormat.getKeySchemaName("_fOoBaR_"), is("io.confluent.ksql.avro_schemas.FoobarKey"));
  }

  private static SimpleColumn column(final String name) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(SqlTypes.BIGINT);
    return column;
  }
}