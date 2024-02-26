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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class FormatInfoTest {

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            FormatInfo.of("DELIMITED", ImmutableMap.of("prop", "x")),
            FormatInfo.of("DELIMITED", ImmutableMap.of("prop", "x"))
        )
        .addEqualityGroup(
            FormatInfo.of("DELIMITED"),
            FormatInfo.of("DELIMITED")
        )
        .addEqualityGroup(
            FormatInfo.of("AVRO")
        )
        .addEqualityGroup(
            FormatInfo.of("DELIMITED", ImmutableMap.of("prop", "|"))
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToStringAvro() {
    // Given:
    final FormatInfo info = FormatInfo.of("AVRO", ImmutableMap.of("property", "something"));

    // When:
    final String result = info.toString();

    // Then:
    assertThat(result, containsString("AVRO"));
    assertThat(result, containsString("something"));
  }

  @Test
  public void shouldGetFormat() {
    assertThat(FormatInfo.of("KAFKA").getFormat(), is("KAFKA"));
  }

  @Test
  public void shouldNotContainSchemaId() {
    // Given:
    final FormatInfo info = FormatInfo.of(
        "AVRO",
        ImmutableMap.of(
            "fullSchemaName", "something",
            "schemaId", "1"));

    // When:
    final FormatInfo copy = info.copyWithoutProperty("schemaId");

    // Then:
    assertThat(copy.getFormat(), is("AVRO"));
    assertThat(copy.getProperties().get("fullSchemaName"), is("something"));
    assertThat(copy.getProperties().containsKey("schemaId"), is(false));
  }

}