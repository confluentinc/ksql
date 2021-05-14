/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import java.math.BigDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class GenericRowTest {

  private final Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  @Test
  public void shouldReturnSize() {
    assertThat(genericRow().size(), is(0));
    assertThat(genericRow(34).size(), is(1));
    assertThat(genericRow(34, ImmutableList.of(1, 2)).size(), is(2));
  }

  @Test
  public void shouldGetValue() {
    assertThat(genericRow(10, 20, 30).get(1), is(20));
  }

  @Test
  public void shouldSetValue() {
    // Given:
    final GenericRow row = genericRow(10, 20, 30);

    // When:
    row.set(1, 1.2);

    // Then:
    assertThat(row.values(), contains(10, 1.2, 30));
  }

  @Test
  public void shouldAppend() {
    // Given:
    final GenericRow row = genericRow(1.3, 492);

    // When:
    row.append(1.2);
    row.appendAll(ImmutableList.of("this", BigDecimal.ONE));

    // Then:
    assertThat(row.values(), contains(1.3, 492, 1.2, "this", BigDecimal.ONE));
  }

  @Test
  public void shouldPrintRowCorrectly() {
    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    final GenericRow genericRow = genericRow(
        "StringColumn", 1, 100000L, true, 1.23,
        ImmutableList.of(10.0, 20.0, 30.0, 40.0, 50.0),
        ImmutableMap.of("key1", 100.0, "key2", 200.0, "key3", 300.0),
        address);

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ 'StringColumn' | 1 | 100000L | true | 1.23 |"
            + " [10.0, 20.0, 30.0, 40.0, 50.0] |"
            + " {key1=100.0, key2=200.0, key3=300.0} |"
            + " Struct{NUMBER=101,STREET=University Ave.,CITY=Palo Alto,STATE=CA,ZIPCODE=94301} ]"));

  }

  @Test
  public void shouldPrintPrimitiveRowCorrectly() {
    final GenericRow genericRow = genericRow("StringColumn", 1, 100000L, true, 1.23, null);

    final String rowString = genericRow.toString();

    assertThat(rowString, is("[ 'StringColumn' | 1 | 100000L | true | 1.23 | null ]"));
  }

  @Test
  public void shouldPrintArrayRowCorrectly() {

    final GenericRow genericRow = genericRow(ImmutableList.of(10.0, 20.0, 30.0, 40.0, 50.0));

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ [10.0, 20.0, 30.0, 40.0, 50.0] ]"));

  }

  @Test
  public void shouldPrintMapRowCorrectly() {

    final GenericRow genericRow = genericRow(
        ImmutableMap.of("key1", 100.0, "key2", 200.0, "key3", 300.0)
    );

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ {key1=100.0, key2=200.0, key3=300.0} ]"));

  }

  @Test
  public void shouldPrintStructRowCorrectly() {
    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    final GenericRow genericRow = genericRow(address);

    final String rowString = genericRow.toString();

    assertThat(rowString, equalTo(
        "[ Struct{NUMBER=101,STREET=University Ave.,CITY=Palo Alto,STATE=CA,ZIPCODE=94301} ]"));

  }

  @Test
  public void shouldHandleRowWithNoElements() {
    final GenericRow genericRow = new GenericRow();

    assertThat(genericRow.size(), is(0));
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void testEquals() {

    new EqualsTester().
        addEqualityGroup(
            new GenericRow(),
            new GenericRow(10)
        )
        .addEqualityGroup(
            GenericRow.genericRow(new Object())
        )
        .addEqualityGroup(
            genericRow("nr"),
            genericRow("nr")
        )
        .addEqualityGroup(
            genericRow(1.0, 94.9238, 1.2550, 0.13242, -1.0285235),
            genericRow(1.0, 94.9238, 1.2550, 0.13242, -1.0285235)
        )
        .testEquals();
  }
}
