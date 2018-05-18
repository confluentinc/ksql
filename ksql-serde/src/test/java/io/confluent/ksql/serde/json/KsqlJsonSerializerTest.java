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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.GenericRow;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class KsqlJsonSerializerTest {

  Schema orderSchema;

  Schema addressSchema;

  Schema itemSchema;
  Schema categorySchema;


  @Before
  public void before() {

    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .field("arraycol".toUpperCase(), SchemaBuilder.array(org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .field("mapcol".toUpperCase(), SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .build();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, Arrays.asList(100.0),
                                 Collections.singletonMap("key1", 100.0));
    GenericRow genericRow = new GenericRow(columns);
    KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo("{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":[100.0],\"MAPCOL\":{\"key1\":100.0}}"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null,
                                 null);
    GenericRow genericRow = new GenericRow(columns);
    KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo("{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":null,\"MAPCOL\":null}"));
  }

  private Schema getSchemaWithStruct() {
    addressSchema = SchemaBuilder.struct()
        .field("NUMBER", Schema.INT64_SCHEMA)
        .field("STREET", Schema.STRING_SCHEMA)
        .field("CITY", Schema.STRING_SCHEMA)
        .field("STATE", Schema.STRING_SCHEMA)
        .field("ZIPCODE", Schema.INT64_SCHEMA)
        .build();

    categorySchema = SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("NAME", Schema.STRING_SCHEMA)
        .build();

    itemSchema = SchemaBuilder.struct()
        .field("ITEMID", Schema.INT64_SCHEMA)
        .field("NAME", Schema.STRING_SCHEMA)
//        .field("CATEGORY", categorySchema)
        .field("CATEGORIES", SchemaBuilder.array(categorySchema))
        .build();

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    Schema schema = schemaBuilder
        .field("ordertime", Schema.INT64_SCHEMA)
        .field("orderid", Schema.INT64_SCHEMA)
        .field("itemid", itemSchema)
        .field("orderunits", Schema.INT32_SCHEMA)
        .field("arraycol",schemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("mapcol", schemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA))
        .field("address", addressSchema).build();

    return schema;
  }

  private GenericRow getGenericRow() {
    List<Object> columns = new ArrayList();
    long currentTime = System.currentTimeMillis();
    currentTime = (long)(1000*Math.random()) + currentTime;
    // ordertime
    columns.add(Long.valueOf(currentTime));

    //orderid
    columns.add(10l);
    //itemid
    Struct category = new Struct(categorySchema);
    category.put("ID", Math.random() > 0.5 ? 1l: 2l);
    category.put("NAME", Math.random() > 0.5 ? "Produce": "Food");

    Struct item = new Struct(itemSchema);
    item.put("ITEMID", 10l);
    item.put("NAME", "Item_10");
    item.put("CATEGORIES", Collections.singletonList(category));

    columns.add(item);

    //units
    columns.add(10);

    Double[] prices = new Double[]{10.0, 20.0, 30.0, 40.0, 50.0};


//      columns.add(prices);
    columns.add(Arrays.asList(prices));

    Map<String, Double> map = new HashMap<>();
    map.put("key1", 10.0);
    map.put("key2", 20.0);
    map.put("key3", 30.0);
    columns.add(map);


    Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101l);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301l);

    columns.add(address);

    GenericRow genericRow = new GenericRow(columns);
    return genericRow;
  }

  @Test
  public void shouldHandleStruct() {
    KsqlJsonSerializer jsonSerializer = new KsqlJsonSerializer(getSchemaWithStruct());
    byte[] bytes = jsonSerializer.serialize("", getGenericRow());
    System.out.println();
  }


}