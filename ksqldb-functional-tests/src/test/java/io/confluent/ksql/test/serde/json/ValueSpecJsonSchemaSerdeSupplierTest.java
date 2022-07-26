package io.confluent.ksql.test.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValueSpecJsonSchemaSerdeSupplierTest {
  @Mock
  private SchemaRegistryClient srClient;

  @Test
  public void shouldSerializeAndDeserializeDecimalsWithOutStrippingTrailingZeros() throws RestClientException, IOException {
    // Given:
    final ValueSpecJsonSchemaSerdeSupplier srSerde = new ValueSpecJsonSchemaSerdeSupplier();

    final Serializer<Object> serializer = srSerde.getSerializer(srClient, false);
    final Deserializer<Object> deserializer = srSerde.getDeserializer(srClient, false);

    when(srClient.getLatestSchemaMetadata("t-value"))
        .thenReturn(new SchemaMetadata(0, 1, ""));
    when(srClient.getSchemaBySubjectAndId("t-value", 0))
        .thenReturn(new JsonSchema("{\n" +
            "  \"properties\": {\n" +
            "    \"B\": {\n" +
            "      \"connect.index\": 0,\n" +
            "      \"oneOf\": [\n" +
            "        {\n" +
            "          \"type\": \"null\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"connect.parameters\": {\n" +
            "            \"connect.decimal.precision\": \"3\",\n" +
            "            \"scale\": \"1\"\n" +
            "          },\n" +
            "          \"connect.type\": \"bytes\",\n" +
            "          \"connect.version\": 1,\n" +
            "          \"title\": \"org.apache.kafka.connect.data.Decimal\",\n" +
            "          \"type\": \"number\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"type\": \"object\"\n" +
            "}"));

    // When:
    final byte[] bytes = serializer.serialize("t",
        ImmutableMap.of("B", new BigDecimal("10.0")));

    // Then:
    assertThat(deserializer.deserialize("t", bytes),
        is(ImmutableMap.of("B", new BigDecimal("10.0"))));
  }
}
