package io.confluent.ksql.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlJsonSchemaDeserializerTest {
  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "@ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";
  private static final String CASE_SENSITIVE_FIELD = "caseField";
  private static final String TIMEFIELD = "TIMEFIELD";
  private static final String DATEFIELD = "DATEFIELD";
  private static final String TIMESTAMPFIELD = "TIMESTAMPFIELD";
  private static final String BYTESFIELD = "BYTESFIELD";

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ORDERID, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ITEMID, Schema.OPTIONAL_STRING_SCHEMA)
      .field(ORDERUNITS, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(CASE_SENSITIVE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ARRAYCOL, SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field(MAPCOL, SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field(TIMEFIELD, ConnectKsqlSchemaTranslator.OPTIONAL_TIME_SCHEMA)
      .field(DATEFIELD, ConnectKsqlSchemaTranslator.OPTIONAL_DATE_SCHEMA)
      .field(TIMESTAMPFIELD, ConnectKsqlSchemaTranslator.OPTIONAL_TIMESTAMP_SCHEMA)
      .field(BYTESFIELD, Schema.OPTIONAL_BYTES_SCHEMA)
      .version(0)
      .build();

  private static final Struct AN_ORDER = new Struct(ORDER_SCHEMA)
      .put(ORDERTIME, 1511897796092L)
      .put(ORDERID, 1L)
      .put(ITEMID, "Item_1")
      .put(ORDERUNITS, 10.0)
      .put(ARRAYCOL, ImmutableList.of(10.0, 20.0))
      .put(MAPCOL, ImmutableMap.of("key1", 10.0))
      .put(CASE_SENSITIVE_FIELD, 1L)
      .put(TIMEFIELD, new java.sql.Time(1000))
      .put(DATEFIELD, new java.sql.Date(864000000L))
      .put(TIMESTAMPFIELD, new java.sql.Timestamp(1000))
      .put(BYTESFIELD, ByteBuffer.wrap(new byte[] {123}));

  private Serializer<Struct> serializer;
  private Deserializer<Struct> deserializer;
  private SchemaRegistryClient schemaRegistryClient;
  private ParsedSchema schema;

  @Before
  public void before() throws Exception {
    schema = (new JsonSchemaTranslator()).fromConnectSchema(ORDER_SCHEMA.schema());

    schemaRegistryClient = new MockSchemaRegistryClient();
    schemaRegistryClient.register(SOME_TOPIC, schema);

    final KsqlJsonSerdeFactory jsonSerdeFactory =
        new KsqlJsonSerdeFactory(new JsonSchemaProperties(ImmutableMap.of()));

    final Serde<Struct> serde = jsonSerdeFactory.createSerde(
        ORDER_SCHEMA,
        new KsqlConfig(ImmutableMap.of()),
        () -> schemaRegistryClient,
        Struct.class,
        false
    );

    serializer = serde.serializer();
    deserializer = serde.deserializer();
  }

  @Test
  public void shouldDeserializeJsonObjectCorrectly() {
    // Given:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, AN_ORDER);

    // When:
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(AN_ORDER));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    assertThat(deserializer.deserialize(SOME_TOPIC, null), is(nullValue()));
  }
}
