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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.ImmutableTester;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PhysicalSchemaTest {

  private static final LogicalSchema SCHEMA_WITH_MULTIPLE_FIELDS = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("f1"), SqlTypes.BOOLEAN)
      .build();

  private static final LogicalSchema SCHEMA_WITH_SINGLE_FIELD = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(LogicalSchema.class, SCHEMA_WITH_MULTIPLE_FIELDS)
        .testAllPublicStaticMethods(PhysicalSchema.class);
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .test(PhysicalSchema.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD, SerdeOption.none()),
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD, SerdeOption.none())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOption.none())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA_WITH_SINGLE_FIELD,
                SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES))
        )
        .testEquals();
  }

  @Test
  public void shouldNotFlattenValueSchemaWithMultipleFields() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOption.none());

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_MULTIPLE_FIELDS.valueConnectSchema()));
  }

  @Test
  public void shouldThrowIfValueWrappingSuppliedForMultiField() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'WRAP_SINGLE_VALUE' is only valid for single-field value schemas");

    // When:
    PhysicalSchema
        .from(SCHEMA_WITH_MULTIPLE_FIELDS, SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldNotFlattenValueSchemaIfNotConfiguredTo() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_SINGLE_FIELD, SerdeOption.none());

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.valueConnectSchema()));
  }

  @Test
  public void shouldFlattenValueSchemasWithOneFieldAndConfiguredTo() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA_WITH_SINGLE_FIELD, SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    // Then:
    assertThat(result.valueSchema().serializedSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.valueConnectSchema().fields().get(0).schema()));
  }
}