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
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.PropertiesList;
import java.util.Collections;
import java.util.Map;
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
  public void shouldUpdateOverwrittenPropertiesCorrectly() {
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList(
        "list properties;",
        ImmutableMap.of(key, "earliest"),
        ImmutableList.of(key)
    );
    final Map<String, Object> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);

    assertThat(properties, hasKey(key + " (LOCAL OVERRIDE)"));
    assertThat(properties.get(key + " (LOCAL OVERRIDE)"), equalTo("earliest"));
  }

  @Test
  public void shouldNotChangeUnwrittenProperty() {
    final String key = KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    final PropertiesList serverPropertiesList = new PropertiesList(
        "list properties;",
        ImmutableMap.of(key, "earliest"),
        Collections.emptyList()
    );
    final Map<String, Object> properties = CliUtils.propertiesListWithOverrides(serverPropertiesList);
    assertThat(properties.get(key), equalTo("earliest"));
  }
}