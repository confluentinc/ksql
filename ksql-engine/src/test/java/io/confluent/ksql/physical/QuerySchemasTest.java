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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.schema.connect.SchemaFormatter;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import java.util.LinkedHashMap;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QuerySchemasTest {

  private static final PersistenceSchema SCHEMA_ONE =
      unwrappedPersistenceSchema(Schema.FLOAT64_SCHEMA);

  private static final PersistenceSchema SCHEMA_TWO =
      unwrappedPersistenceSchema(Schema.OPTIONAL_INT32_SCHEMA);

  private static final PersistenceSchema SCHEMA_THREE =
      unwrappedPersistenceSchema(Schema.STRING_SCHEMA);

  private static final String SCHEMA_ONE_TEXT = "{if you squint, this looks like schema one}";
  private static final String SCHEMA_TWO_TEXT = "{better looking than schema one}";
  private static final String SCHEMA_THREE_TEXT =
      "{the pinnacle of schemas: De La Soul always said 3 is the magic number}";

  @Mock
  private SchemaFormatter schemaFormatter;
  private QuerySchemas schemas;

  @Before
  public void setUp() {
    final LinkedHashMap<String, PersistenceSchema> orderedSchemas = linkedMapOf(
        "thing one", SCHEMA_ONE,
        "thing two", SCHEMA_TWO,
        "thing three", SCHEMA_THREE
    );

    schemas = new QuerySchemas(orderedSchemas, schemaFormatter);

    when(schemaFormatter.format(SCHEMA_ONE.serializedSchema())).thenReturn(SCHEMA_ONE_TEXT);
    when(schemaFormatter.format(SCHEMA_TWO.serializedSchema())).thenReturn(SCHEMA_TWO_TEXT);
    when(schemaFormatter.format(SCHEMA_THREE.serializedSchema())).thenReturn(SCHEMA_THREE_TEXT);
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

  @Test
  public void shouldBeStrictAboutOptionals() {
    // When:
    final QuerySchemas optionals = QuerySchemas.of(linkedMapOf(
        "a", Schema.OPTIONAL_INT32_SCHEMA,
        "b", SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build(),
        "c", SchemaBuilder
            .map(Schema.OPTIONAL_FLOAT64_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .optional()
            .build(),
        "d", SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build()
    ));

    // Then:
    assertThat(optionals.toString(), is(""
        + "a = INT" + System.lineSeparator()
        + "b = ARRAY<VARCHAR>" + System.lineSeparator()
        + "c = MAP<DOUBLE, BOOLEAN>" + System.lineSeparator()
        + "d = STRUCT<f0 BIGINT>"
    ));
  }

  @Test
  public void shouldBeStrictAboutNonOptionals() {
    // When:
    final QuerySchemas nonOptionals = QuerySchemas.of(linkedMapOf(
        "a", Schema.INT32_SCHEMA,
        "b", SchemaBuilder
            .array(Schema.STRING_SCHEMA)
            .build(),
        "c", SchemaBuilder
            .map(Schema.FLOAT64_SCHEMA, Schema.BOOLEAN_SCHEMA)
            .build(),
        "d", SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder.INT64_SCHEMA)
            .build()
    ));

    // Then:
    assertThat(nonOptionals.toString(), is(""
        + "a = INT NOT NULL" + System.lineSeparator()
        + "b = ARRAY<VARCHAR NOT NULL> NOT NULL" + System.lineSeparator()
        + "c = MAP<DOUBLE NOT NULL, BOOLEAN NOT NULL> NOT NULL" + System.lineSeparator()
        + "d = STRUCT<f0 BIGINT NOT NULL> NOT NULL"
    ));
  }

  private static LinkedHashMap<String, PersistenceSchema> linkedMapOf(final Object... e) {

    assertThat("odd param count", e.length % 2, is(0));

    final LinkedHashMap<String, PersistenceSchema> map = new LinkedHashMap<>();

    for (int idx = 0; idx < e.length; ) {
      final Object key = e[idx++];
      Object value = e[idx++];

      assertThat("key must be String", key, instanceOf(String.class));

      if (value instanceof ConnectSchema) {
        value = unwrappedPersistenceSchema((ConnectSchema) value);
      }

      assertThat("value must be Schema", value, instanceOf(PersistenceSchema.class));

      map.put((String) key, (PersistenceSchema) value);
    }

    return map;
  }

  private static PersistenceSchema unwrappedPersistenceSchema(final Schema fieldSchema) {
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", fieldSchema)
        .build();

    return PersistenceSchema.from(connectSchema, true);
  }
}