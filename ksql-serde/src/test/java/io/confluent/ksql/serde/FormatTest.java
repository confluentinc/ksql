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
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class FormatTest {

  @Test
  public void shouldCreateFromString() {
    assertThat(Format.of("JsoN"), is(Format.JSON));
    assertThat(Format.of("AvRo"), is(Format.AVRO));
    assertThat(Format.of("Delimited"), is(Format.DELIMITED));
  }

  @Test
  public void shouldHaveCorrectToString() {
    assertThat(Format.JSON.toString(), is("JSON"));
    assertThat(Format.AVRO.toString(), is("AVRO"));
    assertThat(Format.DELIMITED.toString(), is("DELIMITED"));
  }

  @Test
  public void shouldThrowOnUnknownFormat() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> Format.of("bob")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown format: bob"));
  }
}