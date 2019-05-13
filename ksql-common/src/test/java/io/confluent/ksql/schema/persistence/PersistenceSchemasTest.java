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

package io.confluent.ksql.schema.persistence;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class PersistenceSchemasTest {

  private static final PersistenceSchema SCHEMA_1 = PersistenceSchema.of(
      (ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA
  );

  private static final PersistenceSchema SCHEMA_2 = PersistenceSchema.of(
      (ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA
  );

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(PersistenceSchema.class, SCHEMA_1)
        .testAllPublicStaticMethods(PersistenceSchemas.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            PersistenceSchemas.of(SCHEMA_1),
            PersistenceSchemas.of(SCHEMA_1))
        .addEqualityGroup(
            PersistenceSchemas.of(SCHEMA_2))
        .testEquals();
  }

  @Test
  public void shouldReturnValueSchema() {
    // Given:
    final PersistenceSchemas schema = PersistenceSchemas.of(SCHEMA_1);

    // Then:
    assertThat(schema.valueSchema(), is(SCHEMA_1));
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final PersistenceSchemas schema = PersistenceSchemas.of(SCHEMA_1);

    // Then:
    assertThat(schema.toString(), is("{valueSchema=" + SCHEMA_1 + "}"));
  }
}