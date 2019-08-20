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

package io.confluent.ksql.serde.delimited;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlDelimitedSerdeFactoryTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlDelimitedSerdeFactory factory;

  @Before
  public void setUp() {
    factory = new KsqlDelimitedSerdeFactory();
  }

  @Test
  public void shouldThrowOnValidateIfArray() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'ARRAY'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfMap() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'MAP'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfStruct() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SchemaBuilder
        .struct()
        .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'STRUCT'");

    // When:
    factory.validate(schema);
  }

  private static PersistenceSchema schemaWithFieldOfType(final Schema fieldSchema) {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("f0", fieldSchema)
        .build();

    final LogicalSchema logicalSchema = LogicalSchema.of(connectSchema, connectSchema);
    final PhysicalSchema physicalSchema = PhysicalSchema.from(logicalSchema, SerdeOption.none());
    return physicalSchema.valueSchema();
  }
}