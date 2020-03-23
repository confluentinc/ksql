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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldSetProperty() {
    // Given:
    final SessionProperties sessionProperties =
        new SessionProperties(new HashMap<>(), mock(KsqlHostInfo.class), mock(URL.class), false);
    final Map<String, Object> properties = sessionProperties.getMutableScopedProperties();

    // When:
    CustomExecutors.SET_PROPERTY.execute(
        engine.configure("SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'none';"),
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    );

    // Then:
    assertThat(properties, hasEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none"));
  }

  @Test
  public void shouldUnSetProperty() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "stream");
    final SessionProperties sessionProperties =
        new SessionProperties(
            Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none"),
            mock(KsqlHostInfo.class),
            mock(URL.class),
            false);
    final Map<String, Object> properties = sessionProperties.getMutableScopedProperties();

    // When:
    CustomExecutors.UNSET_PROPERTY.execute(
        engine.configure("UNSET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "';"),
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    );

    // Then:
    assertThat(properties, not(hasKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));
  }
}
