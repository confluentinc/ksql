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

import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF;
import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF_NO_SOURCE;
import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF_NO_SOURCE_TXT;
import static io.confluent.ksql.parser.json.ColumnRefTestCase.COLUMN_REF_TXT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.io.IOException;
import org.junit.Test;

public class ColumnRefDeserializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldDeserializeColumnRef() throws IOException {
    assertThat(
        MAPPER.readValue(COLUMN_REF_TXT, ColumnRef.class),
        equalTo(COLUMN_REF)
    );
  }

  @Test
  public void shouldDeserializeColumnRefWithNoSource() throws IOException {
    assertThat(
        MAPPER.readValue(COLUMN_REF_NO_SOURCE_TXT, ColumnRef.class),
        equalTo(COLUMN_REF_NO_SOURCE)
    );
  }

  @Test
  public void shouldDeserializeColumnRefThatNeedsQuotes() throws IOException {
    assertThat(
        MAPPER.readValue(COLUMN_REF_NEEDS_QUOTES_TXT, ColumnRef.class),
        equalTo(COLUMN_REF_NEEDS_QUOTES)
    );
  }
}