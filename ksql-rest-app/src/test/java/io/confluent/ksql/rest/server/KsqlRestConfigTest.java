/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;


import static io.confluent.ksql.rest.server.KsqlRestConfig.ADVERTISED_LISTENER_CONFIG;
import static io.confluent.rest.RestConfig.LISTENERS_CONFIG;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestConfigTest {

  private static final Map<String, Object> MIN_VALID_CONFIGS = ImmutableMap.<String, Object>builder()
      .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      .put(LISTENERS_CONFIG, "http://localhost:8088")
      .build();

  private static final String QUOTED_INTER_NODE_LISTENER_CONFIG =
      "'" + ADVERTISED_LISTENER_CONFIG + "'";

  private static final String QUOTED_FIRST_LISTENER_CONFIG =
      "first '" + LISTENERS_CONFIG + "'";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Function<URL, Integer> portResolver;
  @Mock
  private Logger logger;

  @Test
  public void shouldGetKsqlConfigProperties() {
    // Given:
    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test")
        .build()
    );

    // When:
    final Map<String, Object> ksqlConfigProperties = config.getKsqlConfigProperties();

    // Then:
    assertThat(ksqlConfigProperties, is(ImmutableMap.of(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        LISTENERS_CONFIG, "http://localhost:8088",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test"))
    );
  }

  // Just a sanity check to make sure that, although they contain identical mappings, successive maps returned by calls
  // to KsqlRestConfig.getOriginals() do not actually return the same object (mutability would then be an issue)
  @Test
  public void shouldReturnDifferentMapOnEachCallToOriginals() {
    // Given:
    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10")
        .build()
    );

    final Map<String, Object> originals1 = config.getOriginals();
    final Map<String, Object> originals2 = config.getOriginals();

    // When:
    originals1.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "99");

    // Then:
    assertThat(originals2.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), is("10"));
  }

  @Test
  public void shouldThrowIfAnyListenerIsInvalidUrl() {
    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value INVALID for configuration "
        + LISTENERS_CONFIG
        + ": Not valid URL: no protocol: INVALID"
    );

    // Given:
    new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, "http://localhost:9875,INVALID")
        .build()
    );
  }

  @Test
  public void shouldThrowIfExplicitInterNodeListenerIsInvalidUrl() {
    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value INVALID for configuration "
        + ADVERTISED_LISTENER_CONFIG
        + ": Not valid URL: no protocol: INVALID"
    );

    // Given:
    new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, "INVALID")
        .build()
    );
  }

  @Test
  public void shouldUseExplicitInterNodeListenerSetToUnresolvableHost() {
    // Given:
    final URL expected = url("https://unresolvable.host:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, expected.toString())
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldUseExplicitInterNodeListenerSetToResolvableHost() {
    // Given:
    final URL expected = url("https://example.com:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, expected.toString())
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldUseExplicitInterNodeListenerIfSetToLocalHost() {
    // Given:
    final URL expected = url("https://localHost:52368");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, expected.toString())
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldUseExplicitInterNodeListenerIfSetToIpv4Loopback() {
    // Given:
    final URL expected = url("https://127.0.0.2:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, expected.toString())
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldUseExplicitInterNodeListenerIfSetToIpv6Loopback() {
    // Given:
    final URL expected = url("https://[::1]:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, expected.toString())
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_INTER_NODE_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldThrowIfExplicitInterNodeListenerHasAutoPortAssignment() {
    // Given:
    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, "https://unresolvable.host:0")
        .build()
    );

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value https://unresolvable.host:0 for configuration "
        + ADVERTISED_LISTENER_CONFIG
        + ": Must have valid port"
    );


    // When:
    config.getInterNodeListener(portResolver, logger);
  }

  @Test
  public void shouldThrowIfExplicitInterNodeListenerHasIpv4WildcardAddress() {
    // Given:
    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, "https://0.0.0.0:12589")
        .build()
    );

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value https://0.0.0.0:12589 for configuration "
        + ADVERTISED_LISTENER_CONFIG
        + ": Can not be wildcard"
    );

    // When:
    config.getInterNodeListener(portResolver, logger);
  }

  @Test
  public void shouldThrowIfExplicitInterNodeListenerHasIpv6WildcardAddress() {
    // Given:
    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .putAll(MIN_VALID_CONFIGS)
        .put(ADVERTISED_LISTENER_CONFIG, "https://[::]:1236")
        .build()
    );

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value https://[::]:1236 for configuration "
        + ADVERTISED_LISTENER_CONFIG
        + ": Can not be wildcard"
    );

    // When:
    config.getInterNodeListener(portResolver, logger);
  }

  @Test
  public void shouldThrowIfOnGetInterNodeListenerIfFirstListenerSetToUnresolvableHost() {
    // Given:
    final URL expected = url("https://unresolvable.host:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, expected.toString() + ",http://localhost:2589")
        .build()
    );

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value "
        + "[https://unresolvable.host:12345, http://localhost:2589] for configuration "
        + LISTENERS_CONFIG
        + ": Could not resolve first host"
    );

    // When:
    config.getInterNodeListener(portResolver, logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerSetToResolvableHost() {
    // Given:
    final URL expected = url("https://example.com:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, expected.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerSetToLocalHost() {
    // Given:
    final URL expected = url("https://localHost:52368");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, expected.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerSetToIpv4Loopback() {
    // Given:
    final URL expected = url("https://127.0.0.2:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, expected.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerSetToIpv6Loopback() {
    // Given:
    final URL expected = url("https://[::1]:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, expected.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyLogsLoopBackWarning(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerWithAutoPortAssignment() {
    // Given:
    final URL autoPort = url("https://example.com:0");

    when(portResolver.apply(any())).thenReturn(2222);

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, autoPort.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    final URL expected = url("https://example.com:2222");

    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerWithIpv4WildcardAddress() {
    // Given:
    final URL wildcard = url("https://0.0.0.0:12589");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, wildcard.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    final URL expected = url("https://" + getLocalHostName() + ":12589");

    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyLogsWildcardWarning(expected);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldResolveInterNodeListenerToFirstListenerWithIpv6WildcardAddress() {
    // Given:
    final URL wildcard = url("https://[::]:12345");

    final KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(LISTENERS_CONFIG, wildcard.toString() + ",http://localhost:2589")
        .build()
    );

    // When:
    final URL actual = config.getInterNodeListener(portResolver, logger);

    // Then:
    final URL expected = url("https://" + getLocalHostName() + ":12345");

    assertThat(actual, is(expected));
    verifyLogsInterNodeListener(expected, QUOTED_FIRST_LISTENER_CONFIG);
    verifyLogsWildcardWarning(expected);
    verifyNoMoreInteractions(logger);
  }

  private void verifyLogsInterNodeListener(final URL listener, final String sourceConfig) {
    verify(logger).info(
        "Using {} config for intra-node communication: {}",
        sourceConfig,
        listener
    );
  }

  private void verifyLogsLoopBackWarning(final URL listener, final String sourceConfig) {
    verify(logger).warn(
        "{} config is set to a loopback address: {}. Intra-node communication will only work "
            + "between nodes running on the same machine.",
        sourceConfig,
        listener
    );
  }

  private void verifyLogsWildcardWarning(final URL listener) {
    verify(logger).warn(
        "{} config uses wildcard address: {}. Intra-node communication will only work "
            + "between nodes running on the same machine.",
        QUOTED_FIRST_LISTENER_CONFIG,
        listener
    );
  }

  private static URL url(final String address) {
    try {
      return new URL(address);
    } catch (final MalformedURLException e) {
      throw new AssertionError("Invalid URL in test: " + address, e);
    }
  }

  private static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new AssertionError("Failed to obtain local host info", e);
    }
  }
}
