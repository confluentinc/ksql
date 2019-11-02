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

package io.confluent.ksql.parser.json;

import static io.confluent.ksql.parser.json.ColumnRefTestCase.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import org.junit.Test;

public class ColumnRefSerializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldSerializeColumnRef() throws IOException {
    assertThat(MAPPER.writeValueAsString(COLUMN_REF), equalTo(COLUMN_REF_TXT));
  }

  @Test
  public void shouldSerializeColumnRefWithNoSource() throws IOException {
    assertThat(MAPPER.writeValueAsString(COLUMN_REF_NO_SOURCE), equalTo(COLUMN_REF_NO_SOURCE_TXT));
  }

  @Test
  public void shouldSerializeColumnRefThatNeedsQuotes() throws IOException {
    assertThat(
        MAPPER.writeValueAsString(COLUMN_REF_NEEDS_QUOTES),
        equalTo(COLUMN_REF_NEEDS_QUOTES_TXT)
    );
  }
}