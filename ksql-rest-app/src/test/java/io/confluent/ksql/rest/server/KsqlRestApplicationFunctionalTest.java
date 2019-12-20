/*
 * Copyright 2019 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.rest.RestConfig;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public class KsqlRestApplicationFunctionalTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER =
      EmbeddedSingleNodeKafkaCluster.build();

  private KsqlRestApplication app;

  @After
  public void tearDown() throws Exception {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  public void shouldResolveListenersWithExplicitAndAutoAssignedPorts() {
    // Given:
    givenAppStartedWith(ImmutableMap
        .of(RestConfig.LISTENERS_CONFIG, "http://127.0.0.1:0,https://localHost:8088"));

    // When:
    final List<URL> listeners = app.getListeners();

    // Then:
    assertThat(listeners, hasSize(2));
    assertThat(listeners.get(0).getProtocol(), is("http"));
    assertThat(listeners.get(0).getHost(), is("127.0.0.1"));
    assertThat(listeners.get(0).getPort(), is(not(0)));
    assertThat(listeners.get(1).getProtocol(), is("https"));
    assertThat(listeners.get(1).getHost(), is("localHost"));
    assertThat(listeners.get(1).getPort(), is(8088));
  }

  @Test
  public void shouldResolveMultipleListenersPerProtocol() {
    // Given:
    givenAppStartedWith(ImmutableMap
        .of(RestConfig.LISTENERS_CONFIG, "http://localhost:0,http://localhost:0"));

    // When:
    final List<URL> listeners = app.getListeners();

    // Then:
    assertThat(listeners, hasSize(2));
    assertThat(listeners.get(0).getProtocol(), is("http"));
    assertThat(listeners.get(0).getHost(), is("localhost"));
    assertThat(listeners.get(0).getPort(), is(not(0)));
    assertThat(listeners.get(1).getProtocol(), is("http"));
    assertThat(listeners.get(1).getHost(), is("localhost"));
    assertThat(listeners.get(1).getPort(), is(not(0)));
  }

  private void givenAppStartedWith(final Map<String, Object> config) {

    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.builder()
        .putAll(config)
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        .build()
    );

    app = KsqlRestApplication.buildApplication(
        restConfig,
        KsqlVersionCheckerAgent::new
    );

    try {
      app.start();
    } catch (Exception e) {
      throw new AssertionError("Failed to start", e);
    }
  }
}