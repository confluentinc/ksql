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

package io.confluent.ksql.connect.supported;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class JdbcSourceTest {

  private final JdbcSource jdbcSource = new JdbcSource();

  @Test
  public void shouldCreateJdbcConnectorWithValidConfigs() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    final Connector expected = new Connector(
        "foo",
        foo -> true,
        foo -> foo,
        DataSourceType.KTABLE,
        null);
    assertThat(maybeConnector, OptionalMatchers.of(is(expected)));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidPrefixTest() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo",
        "topic.prefix", "foo-"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    assertThat(
        "expected match",
        maybeConnector.map(connector -> connector.matches("foo-bar")).orElse(false));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidMapToSource() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "name",
        "topic.prefix", "foo-"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    assertThat(
        maybeConnector.map(connector -> connector.mapToSource("foo-bar")).orElse(null),
        is("name_bar"));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidConfigsAndSMT() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo",
        "transforms", "foobar,createKey",
        "transforms.createKey.type", "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.createKey.field", "key"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    final Connector expected = new Connector(
        "foo",
        foo -> true,
        foo -> foo,
        DataSourceType.KTABLE,
        "key");
    assertThat(maybeConnector, OptionalMatchers.of(is(expected)));
  }

  @Test
  public void shouldResolveJdbcSourceConfigsTemplate() {
    // Given:
    final Map<String, String> originals = ImmutableMap.<String, String>builder()
        .put(Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS)
        .put("transforms", "foo")
        .put("key", "id")
        .build();

    // When:
    final Map<String, String> resolved = jdbcSource.resolveConfigs(originals);

    // Then:
    assertThat(
        resolved,
        is(ImmutableMap.<String, String>builder()
            .put(Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS)
            .put("transforms", "foo,ksqlCreateKey,ksqlExtractString")
            .put("transforms.ksqlCreateKey.type", "org.apache.kafka.connect.transforms.ValueToKey")
            .put("transforms.ksqlCreateKey.fields", "id")
            .put("transforms.ksqlExtractString.type", "org.apache.kafka.connect.transforms.ExtractField$Key")
            .put("transforms.ksqlExtractString.field", "id")
            .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
            .put("tasks.max", "1")
            .build()));
  }

}