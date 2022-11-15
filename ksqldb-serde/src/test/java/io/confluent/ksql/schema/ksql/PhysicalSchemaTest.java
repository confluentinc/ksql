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

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
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
        .setDefault(SerdeFeatures.class, SerdeFeatures.of())
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
            PhysicalSchema.from(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of()),
            PhysicalSchema.from(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(diffSchema, SerdeFeatures.of(), SerdeFeatures.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA, SerdeFeatures.of(UNWRAP_SINGLES), SerdeFeatures.of())
        )
        .addEqualityGroup(
            PhysicalSchema.from(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of(WRAP_SINGLES))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnKeySchema() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA, SerdeFeatures.of(UNWRAP_SINGLES), SerdeFeatures.of());

    // Then:
    assertThat(result.keySchema(), is(PersistenceSchema.from(
        SCHEMA.key(),
        SerdeFeatures.of(UNWRAP_SINGLES)
    )));
  }

  @Test
  public void shouldReturnValueSchema() {
    // When:
    final PhysicalSchema result = PhysicalSchema
        .from(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of(WRAP_SINGLES));

    // Then:
    assertThat(result.valueSchema(), is(PersistenceSchema.from(
        SCHEMA.value(),
        SerdeFeatures.of(WRAP_SINGLES)
    )));
  }
}