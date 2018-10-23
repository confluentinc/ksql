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

package io.confluent.ksql.function.udf.structfieldextractor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.KsqlFunctionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class FetchFieldFromStructTest {

  private final FetchFieldFromStruct fetchFieldFromStruct = new FetchFieldFromStruct();

  final private Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  private Struct getStruct() {
    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);
    return address;
  }

  @Test
  public void shouldReturnCorrectField() {
    assertThat(fetchFieldFromStruct.evaluate(getStruct(), "NUMBER"), equalTo(101L));
  }

  @Test (expected = KsqlFunctionException.class)
  public void shouldFailIfFirstArgIsNotStruct() {
    fetchFieldFromStruct.evaluate(getStruct().get("STATE"), "STATE");
  }

  @Test
  public void shouldReturnNullIfFirstArgIsNull() {
    assertThat(fetchFieldFromStruct.evaluate(null, "NUMBER"), nullValue());
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfArgSizeIsNot2() {
    fetchFieldFromStruct.evaluate();
  }

}