/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListPropertiesExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldListProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.prepare("LIST PROPERTIES;"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(),
        equalTo(engine.getKsqlConfig().getAllConfigPropsWithSecretsObfuscated()));
    assertThat(properties.getOverwrittenProperties(), is(empty()));
  }

  @Test
  public void shouldListPropertiesWithOverrides() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.prepare("LIST PROPERTIES;"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of("auto.offset.reset", "latest")
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(),
        hasEntry("ksql.streams.auto.offset.reset", "latest"));
    assertThat(properties.getOverwrittenProperties(), hasItem("ksql.streams.auto.offset.reset"));
  }

  @Test
  public void shouldNotListSslProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        engine.prepare("LIST PROPERTIES;"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(), not(hasKey(isIn(KsqlConfig.SSL_CONFIG_NAMES))));
  }


}
