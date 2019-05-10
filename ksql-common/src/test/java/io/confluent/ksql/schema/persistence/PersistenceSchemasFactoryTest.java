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
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class PersistenceSchemasFactoryTest {

  private static final KsqlConfig UNWRAPPED_VALUE_CONFIG = new KsqlConfig(ImmutableMap.of(
      KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
  ));

  private static final KsqlConfig WRAPPED_VALUE_CONFIG = new KsqlConfig(ImmutableMap.of(
      KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
  ));

  private static final KsqlSchema SCHEMA_WITH_MULTIPLE_FIELDS = KsqlSchema.of(SchemaBuilder.struct()
      .field("f0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build());

  private static final KsqlSchema SCHEMA_WITH_SINGLE_FIELD = KsqlSchema.of(SchemaBuilder.struct()
      .field("f0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build());

  @Test
  public void shouldNPE() {
    new NullPointerTester()
        .setDefault(KsqlSchema.class, mock(KsqlSchema.class))
        .setDefault(KsqlConfig.class, new KsqlConfig(ImmutableMap.of()))
        .testAllPublicStaticMethods(PersistenceSchemasFactory.class);
  }

  @Test
  public void shouldNotFlattenValueSchemaWithMultipleFields() {
    // When:
    final PersistenceSchemas result = PersistenceSchemasFactory
        .from(SCHEMA_WITH_MULTIPLE_FIELDS, UNWRAPPED_VALUE_CONFIG);

    // Then:
    assertThat(result.valueSchema().getConnectSchema(),
        is(SCHEMA_WITH_MULTIPLE_FIELDS.getSchema()));
  }

  @Test
  public void shouldNotFlattenValueSchemaIfNotConfiguredTo() {
    // When:
    final PersistenceSchemas result = PersistenceSchemasFactory
        .from(SCHEMA_WITH_SINGLE_FIELD, WRAPPED_VALUE_CONFIG);

    // Then:
    assertThat(result.valueSchema().getConnectSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.getSchema()));
  }

  @Test
  public void shouldFlattenValueSchemasWithOneFieldAndConfiguredTo() {
    // When:
    final PersistenceSchemas result = PersistenceSchemasFactory
        .from(SCHEMA_WITH_SINGLE_FIELD, UNWRAPPED_VALUE_CONFIG);

    // Then:
    assertThat(result.valueSchema().getConnectSchema(),
        is(SCHEMA_WITH_SINGLE_FIELD.fields().get(0).schema()));
  }
}