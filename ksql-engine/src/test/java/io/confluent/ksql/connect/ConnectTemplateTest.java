/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.ValueToKey;
import org.junit.Test;

public class ConnectTemplateTest {

  @Test
  public void shouldResolveJdbcSourceConfigs() {
    // Given:
    final Map<String, String> originals = ImmutableMap.<String, String>builder()
        .put(Connectors.CONNECTOR_CLASS, Connectors.JDBC_SOURCE_CLASS)
        .put("transforms", "foo")
        .put("key", "id")
        .build();

    // When:
    final Map<String, String> resolved = ConnectTemplate.resolve(originals);

    // Then:
    assertThat(
        resolved,
        is(ImmutableMap.<String, String>builder()
            .put(Connectors.CONNECTOR_CLASS, Connectors.JDBC_SOURCE_CLASS)
            .put("transforms", "foo,ksqlCreateKey,ksqlExtractString")
            .put("transforms.ksqlCreateKey.type", "org.apache.kafka.connect.transforms.ValueToKey")
            .put("transforms.ksqlCreateKey.fields", "id")
            .put("transforms.ksqlExtractString.type", "org.apache.kafka.connect.transforms.ExtractField$Key")
            .put("transforms.ksqlExtractString.field", "id")
            .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
            .put("tasks.max", "1")
            .build()));
  }

  @Test
  public void shouldDoNothingWithUnknownConnector() {
    // Given:
    final Map<String, String> originals = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, "unknown"
    );

    // When:
    final Map<String, String> resolved = ConnectTemplate.resolve(originals);

    // Then:
    assertThat(resolved, sameInstance(originals));
  }

}