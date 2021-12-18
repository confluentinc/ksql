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

package io.confluent.ksql.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class JsonSchemaPropertiesTest {

  @Test
  public void shouldGetSupportedProperties() {
    // Given:
    final JsonSchemaProperties properties = new JsonSchemaProperties(ImmutableMap.of());

    // When:
    final ImmutableSet<String> supportedProperties = properties.getSupportedProperties();

    // Then:
    assertThat(supportedProperties, is(JsonSchemaProperties.SUPPORTED_PROPERTIES));
  }

  @Test
  public void shouldGetFullSchemaName() {
    // Given:
    final JsonSchemaProperties properties = new JsonSchemaProperties(
        ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "schema"));

    // When: Then:
    assertThat(properties.getFullSchemaName(), is("schema"));
  }

  @Test
  public void shouldGetDefaultFullSchemaName() {
    // Given:
    final JsonSchemaProperties properties = new JsonSchemaProperties(ImmutableMap.of());

    // When: Then:
    assertThat(properties.getFullSchemaName(), is(nullValue()));
  }

  @Test
  public void shouldThrowWithUnsupportedProperty() {
    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> new JsonSchemaProperties(ImmutableMap.of("some_property", "value")));

    // Then:
    assertThat(e.getMessage(), is("JSON_SR does not support the following configs: [some_property]"));
  }
}