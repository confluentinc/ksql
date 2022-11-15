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

package io.confluent.ksql.serde.avro;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroSchemaTranslatorTest {

  @Mock
  private AvroProperties formatProps;
  private AvroSchemaTranslator translator;

  @Before
  public void setUp() {
    when(formatProps.getFullSchemaName()).thenReturn("FullSchemaName");
    translator = new AvroSchemaTranslator(formatProps);
  }

  @Test
  public void shouldThrowWhenBuildingAvroSchemafSchemaContainsInvalidAvroNames() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("2Bad", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> translator.fromConnectSchema(schema)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema is not compatible with Avro: Illegal initial character: 2Bad"));
  }
}