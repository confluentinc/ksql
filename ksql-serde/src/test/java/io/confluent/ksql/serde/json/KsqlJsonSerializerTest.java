/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.json;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class KsqlJsonSerializerTest {

  private Schema orderSchema;

  private Schema addressSchema;

  private Schema itemSchema;
  private Schema categorySchema;


  @Before
  public void before() {

    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("arraycol".toUpperCase(),
            SchemaBuilder.array(org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("mapcol".toUpperCase(), SchemaBuilder
            .map(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA,
                org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .build();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    final List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, Arrays.asList(100.0),
        Collections.singletonMap("key1", 100.0));
    final GenericRow genericRow = new GenericRow(columns);
    final KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    final byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    final String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo(
        "{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":[100.0],\"MAPCOL\":{\"key1\":100.0}}"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    final List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null,
        null);
    final GenericRow genericRow = new GenericRow(columns);
    final KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    final byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    final String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo(
        "{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":null,\"MAPCOL\":null}"));
  }

  private Schema getSchemaWithStruct() {
    addressSchema = SchemaBuilder.struct()
        .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
        .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();

    categorySchema = SchemaBuilder.struct()
        .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    itemSchema = SchemaBuilder.struct()
        .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CATEGORIES", SchemaBuilder.array(categorySchema).optional().build())
        .optional().build();

    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final Schema schema = schemaBuilder
        .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid", Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid", itemSchema)
        .field("orderunits", Schema.OPTIONAL_INT32_SCHEMA)
        .field("arraycol", schemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("mapcol", schemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("address", addressSchema).build();

    return schema;
  }

  private GenericRow getGenericRow() {
    final List<Object> columns = new ArrayList();
    long currentTime = System.currentTimeMillis();
    currentTime = (long) (1000 * Math.random()) + currentTime;
    // ordertime
    columns.add(Long.valueOf(currentTime));

    //orderid
    columns.add(10L);
    //itemid
    final Struct category = new Struct(categorySchema);
    category.put("ID", Math.random() > 0.5 ? 1L : 2L);
    category.put("NAME", Math.random() > 0.5 ? "Produce" : "Food");

    final Struct item = new Struct(itemSchema);
    item.put("ITEMID", 10l);
    item.put("NAME", "Item_10");
    item.put("CATEGORIES", Collections.singletonList(category));

    columns.add(item);

    //units
    columns.add(10);

    final Double[] prices = new Double[]{10.0, 20.0, 30.0, 40.0, 50.0};

    columns.add(Arrays.asList(prices));

    final Map<String, Double> map = new HashMap<>();
    map.put("key1", 10.0);
    map.put("key2", 20.0);
    map.put("key3", 30.0);
    columns.add(map);

    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    columns.add(address);

    final GenericRow genericRow = new GenericRow(columns);
    return genericRow;
  }

  @Test
  public void shouldHandleStruct() throws IOException {
    final KsqlJsonSerializer jsonSerializer = new KsqlJsonSerializer(getSchemaWithStruct());
    final GenericRow genericRow = getGenericRow();
    final byte[] bytes = jsonSerializer.serialize("", genericRow);
    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode jsonNode = objectMapper.readTree(bytes);
    assertThat(jsonNode.size(), equalTo(7));
    assertThat(jsonNode.get("ordertime").asLong(), equalTo(genericRow.getColumns().get(0)));
    assertThat(jsonNode.get("itemid").get("NAME").asText(), equalTo("Item_10"));
  }


}