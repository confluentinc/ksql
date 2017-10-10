/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.util.KsqlConfig;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class KsqlDelimitedDeserializerTest {

  private final Schema schema = SchemaBuilder.struct().field("test", SchemaBuilder.STRING_SCHEMA).build();
  private final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema);

  @Test
  public void shouldReturnNullForBadDataByDefault() {
    assertThat(deserializer.deserialize("topic", "".getBytes()), nullValue());
  }

  @Test(expected = SerializationException.class)
  public void shouldThrowSerializationExceptionWhenFailOnErrorIsTrue() {
    deserializer.configure(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, true), false);
    deserializer.deserialize("topic", "".getBytes());
  }


}