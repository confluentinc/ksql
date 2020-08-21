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

import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.array;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.map;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.struct;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class KsqlDelimitedSerdeFactoryTest {

  private KsqlDelimitedSerdeFactory factory;

  @Before
  public void setUp() {
    factory = new KsqlDelimitedSerdeFactory(Optional.of(Delimiter.of(',')));
  }

  @Test
  public void shouldThrowOnValidateIfArray() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(array(STRING));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'DELIMITED' format does not support type 'ARRAY'"));
  }

  @Test
  public void shouldThrowOnValidateIfMap() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(map(STRING));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'DELIMITED' format does not support type 'MAP'"));
  }

  @Test
  public void shouldThrowOnValidateIfStruct() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(struct()
        .field("f0", STRING)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'DELIMITED' format does not support type 'STRUCT'"));
  }

  private static PersistenceSchema schemaWithFieldOfType(final SqlType fieldSchema) {
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), fieldSchema)
        .valueColumn(ColumnName.of("v0"), fieldSchema)
        .build();

    final PhysicalSchema physicalSchema = PhysicalSchema.from(schema, SerdeOptions.of());
    return physicalSchema.valueSchema();
  }
}