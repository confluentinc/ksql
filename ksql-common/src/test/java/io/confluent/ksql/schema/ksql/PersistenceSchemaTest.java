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
import org.junit.Test;

public class PersistenceSchemaTest {

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(ConnectSchema.class, (ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA)
        .testAllPublicStaticMethods(PersistenceSchema.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            PersistenceSchema.of((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA),
            PersistenceSchema.of((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA))
        .addEqualityGroup(
            PersistenceSchema.of((ConnectSchema) Schema.INT32_SCHEMA))
        .testEquals();
  }

  @Test
  public void shouldReturnSchema() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .of((ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA);

    // Then:
    assertThat(schema.getConnectSchema(), is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .of((ConnectSchema) Schema.OPTIONAL_FLOAT64_SCHEMA);

    // Then:
    assertThat(schema.toString(), is("Persistence{DOUBLE}"));
  }

  @Test
  public void shouldIncludeNotNullInToString() {
    // Given:
    final PersistenceSchema schema = PersistenceSchema
        .of((ConnectSchema) Schema.FLOAT64_SCHEMA);

    // Then:
    assertThat(schema.toString(), is("Persistence{DOUBLE NOT NULL}"));
  }
}