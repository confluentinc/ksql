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

package io.confluent.ksql.rest.server.validation;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyValidatorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailOnUnknownSetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET 'consumer.invalid'='value';",
            new SetProperty(Optional.empty(), "consumer.invalid", "value")),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        new HashMap<>()
    );
  }

  @Test
  public void shouldAllowSetKnownProperty() {
    // No exception when:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "' = '1';",
            new SetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "1")),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        new HashMap<>()
    );
  }


  @Test
  public void shouldFailOnInvalidSetPropertyValue() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Invalid value invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "' = 'invalid';",
            new SetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "invalid")),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        new HashMap<>()
    );
  }

  @Test
  public void shouldFailOnUnknownUnsetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.UNSET_PROPERTY.validate(
        PreparedStatement.of(
            "UNSET 'consumer.invalid';",
            new UnsetProperty(Optional.empty(), "consumer.invalid")),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        new HashMap<>()
    );
  }

  @Test
  public void shouldAllowUnsetKnownProperty() {
    // No exception when:
    CustomValidators.UNSET_PROPERTY.validate(
        PreparedStatement.of(
            "UNSET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "';",
            new UnsetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)),
        engine.getEngine(),
        engine.getServiceContext(),
        engine.getKsqlConfig(),
        new HashMap<>()
    );
  }
}
