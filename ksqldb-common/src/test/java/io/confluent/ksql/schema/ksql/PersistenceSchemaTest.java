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
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.serde.SerdeFeature;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class PersistenceSchemaTest {

  private static final ConnectSchema SINGLE_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final ConnectSchema MULTI_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(EnabledSerdeFeatures.class, EnabledSerdeFeatures.of())
        .setDefault(ConnectSchema.class, SINGLE_FIELD_SCHEMA)
        .testAllPublicStaticMethods(PersistenceSchema.class);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            PersistenceSchema.from(SINGLE_FIELD_SCHEMA, EnabledSerdeFeatures.of()),
            PersistenceSchema.from(SINGLE_FIELD_SCHEMA, EnabledSerdeFeatures.of())
        )
        .addEqualityGroup(
            PersistenceSchema
                .from(SINGLE_FIELD_SCHEMA, EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES))
        )
        .addEqualityGroup(
            PersistenceSchema.from(MULTI_FIELD_SCHEMA, EnabledSerdeFeatures.of())
        )
        .testEquals();
  }

  @Test
  public void shouldReturnSchema() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(SINGLE_FIELD_SCHEMA, EnabledSerdeFeatures.of());

    // Then:
    assertThat(schema.connectSchema(), is(SINGLE_FIELD_SCHEMA));
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(SINGLE_FIELD_SCHEMA, EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES));

    // Then:
    assertThat(schema.toString(),
        is("Persistence{schema=STRUCT<f0 VARCHAR> NOT NULL, features=[WRAP_SINGLES]}"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoneStructSchema() {
    PersistenceSchema.from((ConnectSchema) Schema.FLOAT64_SCHEMA, EnabledSerdeFeatures.of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnWrapIfMultipleFields() {
    PersistenceSchema.from(MULTI_FIELD_SCHEMA, EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnwrapIfMultipleFields() {
    PersistenceSchema
        .from(MULTI_FIELD_SCHEMA, EnabledSerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));
  }
}