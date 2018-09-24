/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.util.CliUtils.PropertyDef;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

/**
 * Unit tests for class {@link CliUtils}.
 *
 * @see CliUtils
 */
public class CliUtilsTest {
  @Test
  public void testGetAvroSchemaThrowsKsqlException() {
    try {
      final CliUtils cliUtils = new CliUtils();
      cliUtils.getAvroSchema("TZGUM?ploV");
      fail("Expecting exception: KsqlException");
    } catch (final KsqlException e) {
      assertThat(CliUtils.class.getName(), equalTo(e.getStackTrace()[0].getClassName()));
    }
  }

  @Test
  public void shouldHandleClientOverwrittenProperties() {
    // Given:
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList("list properties;",
        ImmutableMap.of(key, "earliest"),
        ImmutableList.of(key),
        Collections.emptyList()
    );

    // When:
    final List<PropertyDef> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);

    // Then:
    assertThat(properties, contains(new PropertyDef(key, "SESSION", "earliest")));
  }

  @Test
  public void shouldHandleServerOverwrittenProperties() {
    // Given:
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList("list properties;",
        ImmutableMap.of(key, "earliest"),
        Collections.emptyList(),
        Collections.emptyList()
    );

    // When:
    final List<PropertyDef> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);

    // Then:
    assertThat(properties, contains(new PropertyDef(key, "SERVER", "earliest")));
  }

  @Test
  public void shouldHandleDefaultProperties() {
    // Given:
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList("list properties;",
        ImmutableMap.of(key, "earliest"),
        Collections.emptyList(),
        ImmutableList.of(key)
    );

    // When:
    final List<PropertyDef> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);

    // Then:
    assertThat(properties, contains(new PropertyDef(key, "", "earliest")));
  }

  @Test
  public void shouldHandlePropertiesWithNullValue() {
    // Given:
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList("list properties;",
        Collections.singletonMap(key, null),
        Collections.emptyList(),
        ImmutableList.of(key)
    );

    // When:
    final List<PropertyDef> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);

    // Then:
    assertThat(properties, contains(new PropertyDef(key, "", "NULL")));
  }
}