/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufSerdeFactoryTest {

  private static final ConnectSchema SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("f0", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA))
      .build();

  @Mock
  private PersistenceSchema schema;

  private ProtobufSerdeFactory factory;

  @Before
  public void setUp() {
    factory = new ProtobufSerdeFactory();

    when(schema.serializedSchema())
        .thenReturn(SCHEMA);
  }

  @Test
  public void shouldThrowOnDecimal() {
    // Given:
    final ConnectSchema schemaWithNestedDecimal = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", SchemaBuilder.array(DecimalUtil.builder(10, 2)))
        .build();

    when(schema.serializedSchema())
        .thenReturn(schemaWithNestedDecimal);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), is("The 'PROTOBUF' format does not support type 'DECIMAL'. "
        + "See https://github.com/confluentinc/ksql/issues/5762."));
  }

  @Test
  public void shouldNotThrowOnNonDecimal() {
    // Given:
    final ConnectSchema schemaWithOutDecimal = (ConnectSchema) SchemaBuilder.struct()
        .field("f0", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA))
        .build();

    when(schema.serializedSchema())
        .thenReturn(schemaWithOutDecimal);

    // When:
    factory.validate(schema);

    // Then (did not throw)
  }
}