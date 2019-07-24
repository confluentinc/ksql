/*
 * Copyright 2019 Confluent Inc.
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonProducerTest {

  private static final String TOPIC_NAME = "some-topic";

  private static final org.apache.kafka.connect.data.Schema KSQL_SCHEMA = SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
      .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f2", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f3", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("f4", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  @Mock
  private org.apache.avro.Schema avroSchema;

  private Serializer<GenericRow> serializer;

  @Before
  public void setUp() {
    serializer = new JsonProducer().getSerializer(avroSchema, KSQL_SCHEMA, TOPIC_NAME);
  }

  @Test
  public void shouldSerializeNull() {
    assertThat(serializer.serialize("topic", null), is(nullValue()));
  }

  @Test
  public void shouldSerializeRow() {
    // Given:
    final GenericRow row = new GenericRow(0, 1L, null, 3.0, "four");

    // When:
    final byte[] result = serializer.serialize("topic", row);

    // Then:
    assertThat(result, is(new byte[]{
        123, 34, 102, 48, 34, 58, 48, 44,
        34, 102, 49, 34, 58, 49, 44, 34,
        102, 50, 34, 58, 110, 117, 108, 108,
        44, 34, 102, 51, 34, 58, 51, 46,
        48, 44, 34, 102, 52, 34, 58, 34,
        102, 111, 117, 114, 34, 125
    }));
  }
}