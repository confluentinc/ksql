package io.confluent.ksql.serde.xml;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class KsqlGenericRowXmlDeserializerTest {
  @Test
  public void deserializeSimple() throws Exception {

    Schema schema = SchemaBuilder.struct()
            .field("order.@time", Schema.INT64_SCHEMA)
            .field("order.@ID", Schema.STRING_SCHEMA)
            .field("order.orderid", Schema.INT64_SCHEMA)
            .field("order.itemid", Schema.STRING_SCHEMA)
            .field("order.orderunits", Schema.FLOAT64_SCHEMA)
            .field("order.orderunits.@timestamp", Schema.INT64_SCHEMA)
            .build();

    KsqlGenericRowXmlDeserializer deserializer = new KsqlGenericRowXmlDeserializer(schema);


    String xmlDocument = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<order ID=\"my-id\" time=\"1511897796092\"><orderid>1</orderid><itemid>item_1</itemid><orderunits timestamp=\"12345678\">10.0</orderunits></order>";

    GenericRow genericRow = deserializer.deserialize("test-topic", xmlDocument.getBytes());

    System.out.println(genericRow);
    assertThat(genericRow.getColumns().size(), equalTo(schema.fields().size()));
  }

}