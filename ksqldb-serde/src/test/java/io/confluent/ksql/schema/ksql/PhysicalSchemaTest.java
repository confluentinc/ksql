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
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.test.util.ImmutableTester;
import org.junit.Test;

public class PhysicalSchemaTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("BOB"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("f1"), SqlTypes.BOOLEAN)
      .build();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(SerdeOptions.class, SerdeOptions.of())
        .setDefault(LogicalSchema.class, SCHEMA)
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
    final LogicalSchema diffSchema = LogicalSchema.builder().build();
    new EqualsTester()
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA, SerdeOptions.of()),
            PhysicalSchema.from(SCHEMA, SerdeOptions.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(diffSchema, SerdeOptions.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA, SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnKeySchema() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA, SerdeOptions.of());

    // Then:
    assertThat(result.keySchema(), is(PersistenceSchema.from(
        SCHEMA.keyConnectSchema(),
        EnabledSerdeFeatures.of()
    )));
  }

  @Test
  public void shouldReturnValueSchema() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA, SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES));

    // Then:
    assertThat(result.valueSchema(), is(PersistenceSchema.from(
        SCHEMA.valueConnectSchema(),
        EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES)
    )));
  }
}