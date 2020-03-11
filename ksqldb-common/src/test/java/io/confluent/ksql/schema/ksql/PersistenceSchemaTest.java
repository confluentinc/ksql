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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class PersistenceSchemaTest {

  private static final ConnectSchema WRAPPED_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final ConnectSchema MULTI_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(ConnectSchema.class, WRAPPED_SCHEMA)
        .testAllPublicStaticMethods(PersistenceSchema.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            PersistenceSchema.from(WRAPPED_SCHEMA, true),
            PersistenceSchema.from(WRAPPED_SCHEMA, true)
        )
        .addEqualityGroup(
            PersistenceSchema.from(WRAPPED_SCHEMA, false)
        )
        .addEqualityGroup(
            PersistenceSchema.from(MULTI_FIELD_SCHEMA, false)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnWrappedSchemaUnchanged() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(WRAPPED_SCHEMA, false);

    // Then:
    assertThat(schema.serializedSchema(), is(WRAPPED_SCHEMA));
  }

  @Test
  public void shouldReturnUnwrappedSchema() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(WRAPPED_SCHEMA, true);

    // Then:
    assertThat(schema.serializedSchema(), is(WRAPPED_SCHEMA.fields().get(0).schema()));
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema.from(WRAPPED_SCHEMA, true);

    // Then:
    assertThat(schema.toString(), is("Persistence{schema=VARCHAR, unwrapped=true}"));
  }

  @Test
  public void shouldIncludeNotNullInToString() {
    // Given:
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", Schema.FLOAT64_SCHEMA)
        .build();

    final PersistenceSchema schema = PersistenceSchema.from(connectSchema, true);

    // Then:
    assertThat(schema.toString(), is("Persistence{schema=DOUBLE NOT NULL, unwrapped=true}"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoneStructSchema() {
    PersistenceSchema.from((ConnectSchema) Schema.FLOAT64_SCHEMA, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnwrapIfMultipleFields() {
    PersistenceSchema.from(MULTI_FIELD_SCHEMA, true);
  }
}