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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class KsqlSchemaTest {

  private static final Schema IMMUTABLE_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;
  private static final SchemaBuilder MUTABLE_SCHEMA = SchemaBuilder.int32();

  @Test
  public void shouldDoNothingToImmutableSchema() {
    assertThat(KsqlSchema.of(IMMUTABLE_SCHEMA).getSchema(), is(sameInstance(IMMUTABLE_SCHEMA)));
  }

  @Test
  public void shouldBuildMutableSchemaIntoImmutableSchema() {
    assertThat(KsqlSchema.of(MUTABLE_SCHEMA).getSchema(), is(instanceOf(ConnectSchema.class)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnMutableStructFields() {
    KsqlSchema.of(nested(
        SchemaBuilder.struct()
            .field("fieldWithMutableSchema", MUTABLE_SCHEMA)
            .build()
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnMutableMapKeys() {
    KsqlSchema.of(nested(
        SchemaBuilder.array(
            SchemaBuilder.map(MUTABLE_SCHEMA, IMMUTABLE_SCHEMA).build()
        ).build()
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnMutableMapValues() {
    KsqlSchema.of(nested(
        SchemaBuilder.map(IMMUTABLE_SCHEMA, MUTABLE_SCHEMA).build()
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnMutableArrayElements() {
    KsqlSchema.of(nested(
        SchemaBuilder.array(MUTABLE_SCHEMA).build()
    ));
  }

  private static Schema nested(final Schema schema) {
    // Nest the schema under test within another layer of schema to ensure checks are deep:
    return SchemaBuilder.array(schema).build();
  }
}