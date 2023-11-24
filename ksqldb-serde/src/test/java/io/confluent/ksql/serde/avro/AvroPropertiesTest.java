/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class AvroPropertiesTest {

  @Test
  public void shouldGetSupportedProperties() {
    // Given:
    final AvroProperties properties = new AvroProperties(ImmutableMap.of());

    // When:
    final ImmutableSet<String> supportedProperties = properties.getSupportedProperties();

    // Then:
    assertThat(supportedProperties, is(AvroProperties.SUPPORTED_PROPERTIES));
  }

  @Test
  public void shouldGetFullSchemaName() {
    // Given:
    final AvroProperties properties = new AvroProperties(
        ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "schema"));

    // When: Then:
    assertThat(properties.getFullSchemaName(), is("schema"));
  }

  @Test
  public void shouldGetDefaultFullSchemaName() {
    // Given:
    final AvroProperties properties = new AvroProperties(ImmutableMap.of());

    // When: Then:
    assertThat(properties.getFullSchemaName(), is(AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME));
  }

  @Test
  public void shouldThrowWithUnsupportedProperty() {
    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> new AvroProperties(ImmutableMap.of("some_property", "value")));

    // Then:
    assertThat(e.getMessage(), is("AVRO does not support the following configs: [some_property]"));
  }
}
