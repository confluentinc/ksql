/**
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
 **/

package io.confluent.ksql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class GenericRowTest {

  private final Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER",Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPrintRowCorrectly() {
    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    final GenericRow genericRow = new GenericRow(ImmutableList.of(
        "StringColumn", 1, 100000L, true, 1.23,
        ImmutableList.of(10.0, 20.0, 30.0, 40.0, 50.0),
        ImmutableMap.of("key1", 100.0, "key2", 200.0, "key3", 300.0),
        address));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ 'StringColumn' | 1 | 100000 | true | 1.23 |"
            + " [10.0, 20.0, 30.0, 40.0, 50.0] |"
            + " {key1=100.0, key2=200.0, key3=300.0} |"
            + " Struct{NUMBER=101,STREET=University Ave.,CITY=Palo Alto,STATE=CA,ZIPCODE=94301} ]"));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPrintPrimitiveRowCorrectly() {
    final GenericRow genericRow = new GenericRow(ImmutableList.of(
        "StringColumn", 1, 100000L, true, 1.23));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ 'StringColumn' | 1 | 100000 | true | 1.23 ]"));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPrintArrayRowCorrectly() {

    final GenericRow genericRow = new GenericRow(ImmutableList.of(
        ImmutableList.of(10.0, 20.0, 30.0, 40.0, 50.0)));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ [10.0, 20.0, 30.0, 40.0, 50.0] ]"));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPrintMapRowCorrectly() {

    final GenericRow genericRow = new GenericRow(ImmutableList.of(
        ImmutableMap.of("key1", 100.0, "key2", 200.0, "key3", 300.0)));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ {key1=100.0, key2=200.0, key3=300.0} ]"));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPrintStructRowCorrectly() {
    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    final GenericRow genericRow = new GenericRow(ImmutableList.of(
        address));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ Struct{NUMBER=101,STREET=University Ave.,CITY=Palo Alto,STATE=CA,ZIPCODE=94301} ]"));

  }

}