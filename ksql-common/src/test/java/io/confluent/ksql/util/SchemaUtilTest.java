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

package io.confluent.ksql.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;

public class SchemaUtilTest {

  @Test
  public void shouldCreateCorrectAvroSchema() throws IOException {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder
        .field("ordertime", Schema.INT64_SCHEMA)
        .field("orderid", Schema.STRING_SCHEMA)
        .field("itemid", Schema.STRING_SCHEMA)
        .field("orderunits", Schema.FLOAT64_SCHEMA)
        .field("arraycol", SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("mapcol", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));
    String avroSchemaString = SchemaUtil.buildAvroSchema(schemaBuilder.build(), "orders");
    assertThat("", avroSchemaString.equals("{\"type\":\"record\",\"name\":\"orders\",\"namespace\":\"ksql\",\"fields\":[{\"name\":\"ordertime\",\"type\":\"long\"},{\"name\":\"orderid\",\"type\":\"string\"},{\"name\":\"itemid\",\"type\":\"string\"},{\"name\":\"orderunits\",\"type\":\"double\"},{\"name\":\"arraycol\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"mapcol\",\"type\":{\"type\":\"map\",\"values\":\"double\"}}]}"));
  }

}
