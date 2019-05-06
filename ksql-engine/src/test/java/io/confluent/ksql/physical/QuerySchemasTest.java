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

package io.confluent.ksql.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.schema.connect.SchemaFormatter;
import java.util.LinkedHashMap;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QuerySchemasTest {

  private static final Schema SCHEMA_ONE = Schema.FLOAT64_SCHEMA;
  private static final Schema SCHEMA_TWO = Schema.OPTIONAL_INT32_SCHEMA;
  private static final Schema SCHEMA_THREE = Schema.STRING_SCHEMA;

  private static final String SCHEMA_ONE_TEXT = "{if you squint, this looks like schema one}";
  private static final String SCHEMA_TWO_TEXT = "{better looking than schema one}";
  private static final String SCHEMA_THREE_TEXT =
      "{the pinnacle of schemas: De La Soul always said 3 is the magic number}";

  @Mock
  private SchemaFormatter schemaFormatter;
  private QuerySchemas schemas;

  @Before
  public void setUp() {
    final LinkedHashMap<String, Schema> orderedSchemas = new LinkedHashMap<>();
    orderedSchemas.put("thing one", SCHEMA_ONE);
    orderedSchemas.put("thing two", SCHEMA_TWO);
    orderedSchemas.put("thing three", SCHEMA_THREE);

    schemas = new QuerySchemas(orderedSchemas, schemaFormatter);

    when(schemaFormatter.format(SCHEMA_ONE)).thenReturn(SCHEMA_ONE_TEXT);
    when(schemaFormatter.format(SCHEMA_TWO)).thenReturn(SCHEMA_TWO_TEXT);
    when(schemaFormatter.format(SCHEMA_THREE)).thenReturn(SCHEMA_THREE_TEXT);
  }

  @Test
  public void shouldSerializeInConsistentOrder() {
    // When:
    final String result = schemas.toString();

    // Then:
    assertThat(result, is(
        "thing one = " + SCHEMA_ONE_TEXT + System.lineSeparator()
            + "thing two = " + SCHEMA_TWO_TEXT + System.lineSeparator()
            + "thing three = " + SCHEMA_THREE_TEXT
    ));
  }
}