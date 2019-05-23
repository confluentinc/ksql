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

package io.confluent.ksql.rest.server.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyOverriderTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailOnUnknownSetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        ConfiguredStatement.of(
        PreparedStatement.of(
            "SET 'consumer.invalid'='value';",
            new SetProperty(Optional.empty(), "consumer.invalid", "value")),
            new HashMap<>(),
            engine.getKsqlConfig()),
        engine.getEngine(),
        engine.getServiceContext()
    );
  }

  @Test
  public void shouldAllowSetKnownProperty() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();

    // When:
    CustomValidators.SET_PROPERTY.validate(
        ConfiguredStatement.of(
        PreparedStatement.of(
            "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'earliest';",
            new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")),
            properties,
            engine.getKsqlConfig()),
        engine.getEngine(),
        engine.getServiceContext()
    );

    // Then:
    assertThat(properties, hasEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
  }


  @Test
  public void shouldFailOnInvalidSetPropertyValue() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Invalid value invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        ConfiguredStatement.of(
        PreparedStatement.of(
             "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'invalid';",
            new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "invalid")),
            new HashMap<>(),
            engine.getKsqlConfig()),
        engine.getEngine(),
        engine.getServiceContext()
    );
  }

  @Test
  public void shouldFailOnUnknownUnsetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.UNSET_PROPERTY.validate(
        ConfiguredStatement.of(
        PreparedStatement.of(
            "UNSET 'consumer.invalid';",
            new UnsetProperty(Optional.empty(), "consumer.invalid")),
            new HashMap<>(),
            engine.getKsqlConfig()),
        engine.getEngine(),
        engine.getServiceContext()
    );
  }

  @Test
  public void shouldAllowUnsetKnownProperty() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // When:
    CustomValidators.UNSET_PROPERTY.validate(
        ConfiguredStatement.of(
        PreparedStatement.of(
            "UNSET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "';",
            new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)),
            properties,
            engine.getKsqlConfig()),
        engine.getEngine(),
        engine.getServiceContext()
    );

    // Then:
    assertThat(properties, not(hasKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));
  }
}
