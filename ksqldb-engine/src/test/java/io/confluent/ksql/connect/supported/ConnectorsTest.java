/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect.supported;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Test;

public class ConnectorsTest {

  @Test
  public void shouldResolveConfigs() {
    // Given:
    final Map<String, String> configs = ImmutableMap.of();

    // When/Then:
    assertThat(
        Connectors.resolve(configs), hasEntry("key.converter", StringConverter.class.getName()));
  }

}