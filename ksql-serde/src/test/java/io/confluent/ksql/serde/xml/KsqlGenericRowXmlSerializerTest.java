package io.confluent.ksql.serde.xml;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class KsqlGenericRowXmlSerializerTest {
  @Test
  public void buildXmlModel() throws Exception {


    Schema schema = SchemaBuilder.struct()
            .field("order.@time", Schema.INT64_SCHEMA)
            .field("order.@ID", Schema.STRING_SCHEMA)
            .field("order.orderid", Schema.INT64_SCHEMA)
            .field("order.itemid", Schema.STRING_SCHEMA)
            .field("order.orderunits", Schema.FLOAT64_SCHEMA)
            .field("order.orderunits.@timestamp", Schema.INT64_SCHEMA)
            .build();



    KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    KsqlGenericRowXmlSerializer xmlSerializer = new KsqlGenericRowXmlSerializer(schema);

    List columns = Arrays.asList(1511897796092L, "my-id", 1L, "item_1", 10.0, 12345678L);
    GenericRow genericRow = new GenericRow(columns);

    byte[] results = xmlSerializer.serialize("test-topic", genericRow);

    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<order ID=\"my-id\" time=\"1511897796092\"><orderid>1</orderid><itemid>item_1</itemid><orderunits timestamp=\"12345678\">10.0</orderunits></order>";
    System.out.println(new String(results));

    assertThat(new String(results), equalTo(expected));

  }

}