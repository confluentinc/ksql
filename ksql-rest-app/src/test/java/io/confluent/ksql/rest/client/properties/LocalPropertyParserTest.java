/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.client.properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.ConfigResolver;
import io.confluent.ksql.config.PropertyValidator;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalPropertyParserTest {

  private static final Object PARSED_VALUE = new Object();
  private static final String PARSED_PROP_NAME = "PARSED";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private PropertyValidator validator;
  @Mock
  private ConfigResolver resolver;
  @Mock
  private ConfigItem configItem;
  private LocalPropertyParser parser;

  @Before
  public void setUp() {
    parser = new LocalPropertyParser(resolver, validator);

    when(configItem.parseValue(any(Object.class)))
        .thenReturn(PARSED_VALUE);

    when(configItem.getPropertyName())
        .thenReturn(PARSED_PROP_NAME);

    when(resolver.resolve(anyString(), anyBoolean()))
        .thenReturn(Optional.of(configItem));
  }

  @Test
  public void shouldNotCallResolverForRunScriptConstant() {
    // When:
    parser.parse(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT, "100");

    // Then:
    verify(resolver, never()).resolve(anyString(), anyBoolean());
  }

  @Test
  public void shouldCallValidatorForRunScriptConstant() {
    // When:
    parser.parse(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT, "something2");

    // Then:
    verify(validator).validate(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT, "something2");
  }

  @Test
  public void shouldCallResolverForOtherProperties() {
    // When:
    parser.parse(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "100");

    // Then:
    verify(resolver).resolve(KsqlConfig.KSQL_SERVICE_ID_CONFIG, true);
  }

  @Test
  public void shouldThrowIfResolverFailsToResolve() {
    // Given:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Not recognizable as ksql, streams, consumer, or producer property: 'Unknown'");

    when(resolver.resolve(anyString(), anyBoolean()))
        .thenReturn(Optional.empty());

    // When:
    parser.parse("Unknown", "100");
  }

  @Test
  public void shouldCallValidatorWithParsedValue() {
    // When:
    parser.parse(ProducerConfig.LINGER_MS_CONFIG, "100");

    // Then:
    verify(validator).validate(PARSED_PROP_NAME, PARSED_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfValidatorThrows() {
    // Given:
    doThrow(new IllegalArgumentException("Boom"))
      .when(validator).validate(anyString(), any(Object.class));

    // When:
    parser.parse(ProducerConfig.LINGER_MS_CONFIG, "100");
  }
}