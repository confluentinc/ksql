/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.datagen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class RowGeneratorTest {

  @Test
  public void shouldGenerateCorrectRow() throws IOException {
    final Generator generator = new Generator(new File("./src/main/resources/orders_schema.avro"), new Random());

    final RowGenerator rowGenerator = new RowGenerator(generator, "orderid");

    final Pair<Struct, GenericRow> rowPair = rowGenerator.generateRow();

    final Struct key = rowPair.getLeft();
    assertThat(key, is(notNullValue()));
    assertThat(key.get("ROWKEY"), is(instanceOf(String.class)));

    assertThat(rowPair.getRight().getColumns(), hasSize(5));
    assertThat(rowPair.getRight().getColumns().get(4), instanceOf(Struct.class));

    final Struct struct = (Struct) rowPair.getRight().getColumns().get(4);
    assertThat(struct.schema().fields(), hasSize(3));
    assertThat(struct.schema().field("city").schema().type(), equalTo(Type.STRING));
    assertThat(struct.schema().field("state").schema().type(), equalTo(Type.STRING));
    assertThat(struct.schema().field("zipcode").schema().type(), equalTo(Type.INT64));
  }
}