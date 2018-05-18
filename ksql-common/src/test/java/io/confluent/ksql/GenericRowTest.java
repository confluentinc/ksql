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

package io.confluent.ksql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericRowTest {

  Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER",Schema.INT64_SCHEMA)
      .field("STREET", Schema.STRING_SCHEMA)
      .field("CITY", Schema.STRING_SCHEMA)
      .field("STATE", Schema.STRING_SCHEMA)
      .field("ZIPCODE", Schema.INT64_SCHEMA)
      .build();

  @Test
  public void shouldPrintRowCorrectly() {
    List columns = new ArrayList();
    columns.add("StringColumn");
    columns.add(1);
    columns.add(100000l);
    columns.add(true);
    columns.add(1.23);

    Double[] prices = new Double[]{10.0, 20.0, 30.0, 40.0, 50.0};


    columns.add(Arrays.asList(prices));

    Map<String, Double> map = new HashMap<>();
    map.put("key1", 100.0);
    map.put("key2", 200.0);
    map.put("key3", 300.0);
    columns.add(map);


    Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101l);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301l);

    columns.add(address);

    GenericRow genericRow = new GenericRow(columns);

    String rowString = genericRow.toString();

    Assert.assertThat(rowString, CoreMatchers.equalTo("[ 'StringColumn' | 1 | 100000 | true | 1.23 | [10.0, 20.0, 30.0, 40.0, 50.0] | {key1=100.0, key2=200.0, key3=300.0} | Struct{NUMBER=101,STREET=University Ave.,CITY=Palo Alto,STATE=CA,ZIPCODE=94301} ]"));

  }

}