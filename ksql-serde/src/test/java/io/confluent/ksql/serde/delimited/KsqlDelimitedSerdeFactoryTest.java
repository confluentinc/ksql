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
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
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
    factory = new KsqlDelimitedSerdeFactory(Optional.of(','));
  }

  @Test
  public void shouldThrowOnValidateIfArray() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.array(SqlTypes.STRING));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'ARRAY'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfMap() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.map(SqlTypes.STRING));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'MAP'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfStruct() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.struct()
        .field("f0", SqlTypes.STRING)
        .build()
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The 'DELIMITED' format does not support type 'STRUCT'");

    // When:
    factory.validate(schema);
  }

  private static PersistenceSchema schemaWithFieldOfType(final SqlType fieldSchema) {
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn("k0", fieldSchema)
        .valueColumn("v0", fieldSchema)
        .build();

    final PhysicalSchema physicalSchema = PhysicalSchema.from(schema, SerdeOption.none());
    return physicalSchema.valueSchema();
  }
}