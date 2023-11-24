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

import static io.confluent.ksql.serde.connect.ConnectProperties.FULL_SCHEMA_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class JsonPropertiesTest {

  @Test
  public void shouldGetSupportedProperties() {
    // Given:
    final JsonProperties properties = new JsonProperties(ImmutableMap.of());

    // When:
    final ImmutableSet<String> supportedProperties = properties.getSupportedProperties();

    // Then:
    assertThat(supportedProperties, is(JsonProperties.SUPPORTED_PROPERTIES));
  }

  @Test
  public void shouldGetFullSchemaName() {
    // Given:
    final JsonProperties properties = new JsonProperties(ImmutableMap.of());

    // When:
    final Exception e = assertThrows(UnsupportedOperationException.class,
        properties::getFullSchemaName);

    // Then:
    assertThat(e.getMessage(), is("JSON does not implement Schema Registry support"));
  }

  @Test
  public void shouldGetDefaultFullSchemaName() {
    // Given:
    final JsonProperties properties = new JsonProperties(ImmutableMap.of());

    // When:
    final Exception e = assertThrows(UnsupportedOperationException.class,
        properties::getDefaultFullSchemaName);

    // Then:
    assertThat(e.getMessage(), is("JSON does not implement Schema Registry support"));
  }

  @Test
  public void shouldThrowWithUnsupportedProperty() {
    // When:
    final Exception e = assertThrows(KsqlException.class,
        () -> new JsonProperties(ImmutableMap.of(FULL_SCHEMA_NAME, "schema")));

    // Then:
    assertThat(e.getMessage(),
        is("JSON does not support the following configs: [" + FULL_SCHEMA_NAME + "]"));
  }
}
