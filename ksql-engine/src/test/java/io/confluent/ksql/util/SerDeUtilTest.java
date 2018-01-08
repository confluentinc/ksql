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

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;


public class SerDeUtilTest {

  String avroSchemaStr = "{\n"
                         + "  \"type\": \"record\",\n"
                         + "  \"name\": \"LOGON\",\n"
                         + "  \"namespace\": \"ORCL.SOE2\",\n"
                         + "  \"fields\": [\n"
                         + "    {\n"
                         + "      \"name\": \"table\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"op_type\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"op_ts\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"current_ts\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"pos\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"LOGON_ID\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"double\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"CUSTOMER_ID\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"double\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    },\n"
                         + "    {\n"
                         + "      \"name\": \"LOGON_DATE\",\n"
                         + "      \"type\": [\n"
                         + "        \"null\",\n"
                         + "        \"string\"\n"
                         + "      ],\n"
                         + "      \"default\": null\n"
                         + "    }\n"
                         + "  ],\n"
                         + "  \"connect.name\": \"ORCL.SOE2.LOGON\"\n"
                         + "}";

  @Test
  public void shouldReturnCorrectKsqlSchema() {
    Schema schema = SerDeUtil.getSchemaFromAvro(avroSchemaStr);
    assertThat("Incorrect schema.", schema.fields().size(), equalTo(8));
    assertThat("Incorrect field schema.", schema.fields().get(0).schema(), equalTo(Schema.STRING_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(1).schema(), equalTo(Schema
                                                                                       .STRING_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(2).schema(), equalTo(Schema
                                                                                       .STRING_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(3).schema(), equalTo(Schema
                                                                                       .STRING_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(4).schema(), equalTo(Schema
                                                                                       .STRING_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(5).schema(), equalTo(Schema
                                                                                       .FLOAT64_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(6).schema(), equalTo(Schema
                                                                                       .FLOAT64_SCHEMA));
    assertThat("Incorrect field schema.", schema.fields().get(7).schema(), equalTo(Schema
                                                                                       .STRING_SCHEMA));
  }


  @Test
  public void shouldFailForUnsupportedUnion() {

    String avroSchemaStr = "{\n"
                           + "  \"type\": \"record\",\n"
                           + "  \"name\": \"LOGON\",\n"
                           + "  \"namespace\": \"ORCL.SOE2\",\n"
                           + "  \"fields\": [\n"
                           + "    {\n"
                           + "      \"name\": \"table\",\n"
                           + "      \"type\": [\n"
                           + "        \"string\"\n"
                           + "      ],\n"
                           + "      \"default\": null\n"
                           + "    },\n"
                           + "    {\n"
                           + "      \"name\": \"LOGON_DATE\",\n"
                           + "      \"type\": [\n"
                           + "        \"null\",\n"
                           + "        \"int\",\n"
                           + "        \"long\"\n"
                           + "      ],\n"
                           + "      \"default\": null\n"
                           + "    }\n"
                           + "  ],\n"
                           + "  \"connect.name\": \"ORCL.SOE2.LOGON\"\n"
                           + "}";

    try {
      Schema schema = SerDeUtil.getSchemaFromAvro(avroSchemaStr);
      fail();
    } catch (KsqlException ksqlException) {
      assertThat(ksqlException.getMessage(), equalTo("Union type cannot have more than two "
                                                        + "types and one of them should be null."));
    }
  }

  @Test
  public void shouldFailForUnionWithNoNull() {

    String avroSchemaStr = "{\n"
                           + "  \"type\": \"record\",\n"
                           + "  \"name\": \"LOGON\",\n"
                           + "  \"namespace\": \"ORCL.SOE2\",\n"
                           + "  \"fields\": [\n"
                           + "    {\n"
                           + "      \"name\": \"table\",\n"
                           + "      \"type\": [\n"
                           + "        \"string\"\n"
                           + "      ],\n"
                           + "      \"default\": null\n"
                           + "    },\n"
                           + "    {\n"
                           + "      \"name\": \"LOGON_DATE\",\n"
                           + "      \"type\": [\n"
                           + "        \"string\",\n"
                           + "        \"long\"\n"
                           + "      ],\n"
                           + "      \"default\": null\n"
                           + "    }\n"
                           + "  ],\n"
                           + "  \"connect.name\": \"ORCL.SOE2.LOGON\"\n"
                           + "}";

    try {
      Schema schema = SerDeUtil.getSchemaFromAvro(avroSchemaStr);
      fail();
    } catch (KsqlException ksqlException) {
      assertThat(ksqlException.getMessage(), equalTo("Union type cannot have more than two "
                                                         + "types and one of them should be null."));
    }
  }

}
