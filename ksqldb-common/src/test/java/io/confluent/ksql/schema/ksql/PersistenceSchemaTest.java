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

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.List;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class PersistenceSchemaTest {

  private static final ConnectSchema SINGLE_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final List<? extends Column> SINGLE_COLUMN = ImmutableList.of(
      Column.of(ColumnName.of("Bob"), SqlTypes.INTEGER, Namespace.VALUE, 0)
  );

  private static final ConnectSchema MULTI_FIELD_SCHEMA = (ConnectSchema) SchemaBuilder
      .struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  private static final List<? extends Column> MULTI_COLUMN = ImmutableList.of(
      Column.of(ColumnName.of("f0"), SqlTypes.BIGINT, Namespace.VALUE, 0),
      Column.of(ColumnName.of("f1"), SqlTypes.BIGINT, Namespace.VALUE, 1)
  );

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(SerdeFeatures.class, SerdeFeatures.of())
        .setDefault(ConnectSchema.class, SINGLE_FIELD_SCHEMA)
        .testAllPublicStaticMethods(PersistenceSchema.class);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            PersistenceSchema.from(SINGLE_COLUMN, SerdeFeatures.of()),
            PersistenceSchema.from(SINGLE_COLUMN, SerdeFeatures.of())
        )
        .addEqualityGroup(
            PersistenceSchema.from(MULTI_COLUMN, SerdeFeatures.of())
        )
        .addEqualityGroup(
            PersistenceSchema
                .from(SINGLE_COLUMN, SerdeFeatures.of(SerdeFeature.WRAP_SINGLES))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnColumns() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(SINGLE_COLUMN, SerdeFeatures.of());

    // Then:
    assertThat(schema.columns(), is(SINGLE_COLUMN));
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .from(SINGLE_COLUMN, SerdeFeatures.of(SerdeFeature.WRAP_SINGLES));

    // Then:
    assertThat(schema.toString(),
        is("Persistence{columns=[`Bob` INTEGER], features=[WRAP_SINGLES]}"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnWrapIfMultipleFields() {
    PersistenceSchema.from(MULTI_COLUMN, SerdeFeatures.of(SerdeFeature.WRAP_SINGLES));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnwrapIfMultipleFields() {
    PersistenceSchema
        .from(MULTI_COLUMN, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));
  }
}