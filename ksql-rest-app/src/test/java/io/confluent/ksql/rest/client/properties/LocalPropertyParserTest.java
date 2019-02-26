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

package io.confluent.ksql.rest.client.properties;

import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.ConfigResolver;
import io.confluent.ksql.config.PropertyValidator;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class LocalPropertyParserTest {

  private static final Object PARSED_VALUE = new Object();
  private static final String PARSED_PROP_NAME = "PARSED";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock(MockType.NICE)
  private PropertyValidator validator;
  @Mock(MockType.NICE)
  private ConfigResolver resolver;
  @Mock(MockType.NICE)
  private ConfigItem configItem;
  private LocalPropertyParser parser;

  @Before
  public void setUp() {
    parser = new LocalPropertyParser(resolver, validator);

    EasyMock.expect(configItem.parseValue(EasyMock.anyObject()))
        .andReturn(PARSED_VALUE)
        .anyTimes();

    EasyMock.expect(configItem.getPropertyName())
        .andReturn(PARSED_PROP_NAME)
        .anyTimes();
  }

  @Test
  public void shouldNotCallResolverForAvroSchemaConstant() {
    // Given:
    EasyMock.expect(resolver.resolve(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andThrow(new AssertionError("resolve called"))
        .anyTimes();

    EasyMock.replay(resolver);

    // When:
    parser.parse(DdlConfig.AVRO_SCHEMA, "100");

    // Then:
    EasyMock.verify(resolver);
  }

  @Test
  public void shouldCallValidatorForAvroSchemaConstant() {
    // Given:
    validator.validate(DdlConfig.AVRO_SCHEMA, "something");
    EasyMock.expectLastCall();
    EasyMock.replay(validator);

    // When:
    parser.parse(DdlConfig.AVRO_SCHEMA, "something");

    // Then:
    EasyMock.verify(validator);
  }

  @Test
  public void shouldNotCallResolverForRunScriptConstant() {
    // Given:
    EasyMock.expect(resolver.resolve(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andThrow(new AssertionError("resolve called"))
        .anyTimes();

    EasyMock.replay(resolver);

    // When:
    parser.parse(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, "100");

    // Then:
    EasyMock.verify(resolver);
  }

  @Test
  public void shouldCallValidatorForRunScriptConstant() {
    // Given:
    validator.validate(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, "something");
    EasyMock.expectLastCall();
    EasyMock.replay(validator);

    // When:
    parser.parse(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, "something");

    // Then:
    EasyMock.verify(validator);
  }

  @Test
  public void shouldCallResolverForProperties() {
    // Given:
    EasyMock.expect(resolver.resolve(KsqlConfig.KSQL_SERVICE_ID_CONFIG, true))
        .andReturn(Optional.of(configItem))
        .once();

    EasyMock.replay(resolver, configItem);

    // When:
    parser.parse(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "100");

    // Then:
    EasyMock.verify(resolver);
  }

  @Test
  public void shouldThrowIfResolverFailsToResolve() {
    // Given:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Not recognizable as ksql, streams, consumer, or producer property: 'Unknown'");

    EasyMock.expect(resolver.resolve(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(Optional.empty())
        .once();

    EasyMock.replay(resolver);

    // When:
    parser.parse("Unknown", "100");

    // Then:
    EasyMock.verify(resolver);
  }

  @Test
  public void shouldCallValidatorWithParsedValue() {
    // Given:
    EasyMock.expect(resolver.resolve(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(Optional.of(configItem))
        .once();

    validator.validate(PARSED_PROP_NAME, PARSED_VALUE);
    EasyMock.expectLastCall();
    EasyMock.replay(validator, configItem, resolver);

    // When:
    parser.parse(ProducerConfig.LINGER_MS_CONFIG, "100");

    // Then:
    EasyMock.verify(validator);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfValidatorThrows() {
    // Given:
    EasyMock.expect(resolver.resolve(EasyMock.anyString(), EasyMock.anyBoolean()))
        .andReturn(Optional.of(configItem))
        .once();

    validator.validate(PARSED_PROP_NAME, PARSED_VALUE);
    EasyMock.expectLastCall().andThrow(new IllegalArgumentException("Boom"));
    EasyMock.replay(validator, configItem, resolver);

    // When:
    parser.parse(ProducerConfig.LINGER_MS_CONFIG, "100");
  }
}