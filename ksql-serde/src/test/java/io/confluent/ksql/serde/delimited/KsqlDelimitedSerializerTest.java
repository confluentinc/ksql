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

package io.confluent.ksql.serde.delimited;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class KsqlDelimitedSerializerTest {

  private KsqlDelimitedSerializer serializer;

  @Before
  public void setUp() {
    serializer = new KsqlDelimitedSerializer();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    // Given:
    final GenericRow genericRow = new GenericRow(Arrays.asList(1511897796092L, 1L, "item_1", 10.0));

    // When:
    final byte[] bytes = serializer.serialize("t1", genericRow);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,10.0"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    // Given:
    final GenericRow genericRow = new GenericRow(Arrays.asList(1511897796092L, 1L, "item_1", null));

    // When:
    final byte[] bytes = serializer.serialize("t1", genericRow);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,"));
  }
}
