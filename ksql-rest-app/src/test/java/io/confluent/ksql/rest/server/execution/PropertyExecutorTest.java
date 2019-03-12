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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import java.util.HashMap;
import java.util.Map;
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
    engine.givenSource(DataSourceType.KSTREAM, "stream");
    final Map<String, Object> properties = new HashMap<>();

    // When:
    CustomExecutors.SET_PROPERTY.execute(
        engine.prepare("SET 'property' = 'value';"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        properties
    );

    // Then:
    assertThat(properties, hasEntry("property", "value"));
  }

  @Test
  public void shouldUnSetProperty() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "stream");
    final Map<String, Object> properties = new HashMap<>();
    properties.put("property", "value");

    // When:
    CustomExecutors.UNSET_PROPERTY.execute(
        engine.prepare("UNSET 'property';"),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        properties
    );

    // Then:
    assertThat(properties, not(hasKey("property")));
  }


}
