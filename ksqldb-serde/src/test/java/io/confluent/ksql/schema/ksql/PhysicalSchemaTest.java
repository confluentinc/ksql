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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.test.util.ImmutableTester;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class PhysicalSchemaTest {

  private static final LogicalSchema SCHEMA_WITH_MULTIPLE_FIELDS = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("f1"), SqlTypes.BOOLEAN)
      .build();

  private static final LogicalSchema SCHEMA_WITH_SINGLE_FIELD = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
      .build();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(SerdeOptions.class, SerdeOptions.of())
        .setDefault(LogicalSchema.class, SCHEMA_WITH_MULTIPLE_FIELDS)
        .testAllPublicStaticMethods(PhysicalSchema.class);
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .test(PhysicalSchema.class);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD, SerdeOptions.of()),
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD, SerdeOptions.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOptions.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD,
                SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES))
        )
        .testEquals();
  }

  @Test
  public void shouldNotFlattenValueSchemaWithMultipleFields() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOptions.of());

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_MULTIPLE_FIELDS.valueConnectSchema()));
  }

  @Test
  public void shouldThrowIfValueWrappingSuppliedForMultiField() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> PhysicalSchema
            .from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas"));
  }

  @Test
  public void shouldNotFlattenValueSchemaIfNotConfiguredTo() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_SINGLE_FIELD, SerdeOptions.of());

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.valueConnectSchema()));
  }

  @Test
  public void shouldFlattenValueSchemasWithOneFieldAndConfiguredTo() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_SINGLE_FIELD, SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.valueConnectSchema().fields().get(0).schema()));
  }
}