/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class DataGenProducerTest {

  @Test
  public void shouldGenerateCorrectRow() throws IOException {
    final DataGenProducer dataGenProducer = new JsonProducer();
    final Generator generator = new Generator(new File("./src/main/resources/orders_schema.avro"), new Random());
    final Properties props = new Properties();

    final Schema addressSchema = SchemaBuilder.struct()
        .field("city", Schema.OPTIONAL_STRING_SCHEMA)
        .field("state", Schema.OPTIONAL_STRING_SCHEMA)
        .field("zipcode", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();


    final Schema ordersSchema = SchemaBuilder.struct()
        .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid", Schema.OPTIONAL_INT32_SCHEMA)
        .field("itemid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("address", addressSchema)
        .optional().build();

    Pair<String, GenericRow> rowPair = dataGenProducer.generateOneGenericRow(generator, new AvroData(1), generator.schema(), ordersSchema, new SessionManager(), "orderid");
    assertThat(rowPair.getLeft(), instanceOf(String.class));
    assertThat(rowPair.getRight().getColumns().size(), equalTo(5));
    assertThat(rowPair.getRight().getColumns().get(4), instanceOf(Struct.class));
    final Struct struct = (Struct) rowPair.getRight().getColumns().get(4);
    assertThat(struct.schema().fields().size(), equalTo(3));
    assertThat(struct.schema().field("city").schema().type(), equalTo(Type.STRING));
    assertThat(struct.schema().field("state").schema().type(), equalTo(Type.STRING));
    assertThat(struct.schema().field("zipcode").schema().type(), equalTo(Type.INT64));
  }
}