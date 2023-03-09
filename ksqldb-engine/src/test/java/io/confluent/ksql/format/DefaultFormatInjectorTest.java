/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.format;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultFormatInjectorTest {

  @Mock
  private CreateSource createSource;

  private ConfiguredStatement<CreateSource> csStatement;

  private DefaultFormatInjector injector;

  @Before
  public void setUp() {
    when(createSource.getName()).thenReturn(SourceName.of("source"));
    when(createSource.getElements()).thenReturn(TableElements.of());
    when(createSource.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, createSource, mock(CreateStream.class)));

    injector = new DefaultFormatInjector();
  }

  @Test
  public void shouldInjectMissingKeyFormat() {
    // Given
    givenConfig(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG, "KAFKA"
    ));
    givenSourceProps(ImmutableMap.of(
        "VALUE_FORMAT", new StringLiteral("JSON")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result.getMaskedStatementText(), containsString("KEY_FORMAT='KAFKA'"));
  }

  @Test
  public void shouldInjectMissingValueFormat() {
    // Given
    givenConfig(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "JSON"
    ));
    givenSourceProps(ImmutableMap.of(
        "KEY_FORMAT", new StringLiteral("KAFKA")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result.getMaskedStatementText(), containsString("VALUE_FORMAT='JSON'"));
  }

  @Test
  public void shouldInjectMissingKeyAndValueFormat() {
    // Given
    givenConfig(ImmutableMap.of(
        KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG, "KAFKA",
        KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "JSON"
    ));
    givenSourceProps(ImmutableMap.of());

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result.getMaskedStatementText(), containsString("KEY_FORMAT='KAFKA'"));
    assertThat(result.getMaskedStatementText(), containsString("VALUE_FORMAT='JSON'"));
  }

  @Test
  public void shouldHandleExplicitKeyAndValueFormat() {
    // Given
    givenConfig(ImmutableMap.of());
    givenSourceProps(ImmutableMap.of(
        "KEY_FORMAT", new StringLiteral("KAFKA"),
        "VALUE_FORMAT", new StringLiteral("JSON")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result, sameInstance(csStatement));
  }

  @Test
  public void shouldHandleExplicitFormat() {
    // Given
    givenConfig(ImmutableMap.of());
    givenSourceProps(ImmutableMap.of(
        "FORMAT", new StringLiteral("KAFKA")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result, sameInstance(csStatement));
  }

  @Test
  public void shouldDefaultToKafkaIfNoExplicitDefaultKeyFormat() {
    // Given
    givenConfig(ImmutableMap.of());
    givenSourceProps(ImmutableMap.of(
        "VALUE_FORMAT", new StringLiteral("JSON")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result.getMaskedStatementText(), containsString("KEY_FORMAT='KAFKA'"));
  }

  @Test
  public void shouldThrowIfMissingDefaultValueFormatConfig() {
    // Given
    givenConfig(ImmutableMap.of());
    givenSourceProps(ImmutableMap.of(
        "KEY_FORMAT", new StringLiteral("KAFKA")
    ));

    // Expect / When
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(csStatement)
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Statement is missing the 'VALUE_FORMAT' property from the WITH clause."));
    assertThat(e.getMessage(), containsString("Either provide one or set a default via the '"
        + KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG + "' config."));
  }

  @Test
  public void shouldInjectUsingConfigOverrides() {
    // Given
    givenConfig(
        ImmutableMap.of(),
        ImmutableMap.of(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "JSON")
    );
    givenSourceProps(ImmutableMap.of(
        "KEY_FORMAT", new StringLiteral("KAFKA")
    ));

    // When
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then
    assertThat(result.getMaskedStatementText(), containsString("VALUE_FORMAT='JSON'"));
  }

  private void givenConfig(final Map<String, Object> additionalConfigProps) {
    givenConfig(additionalConfigProps, ImmutableMap.of());
  }

  private void givenConfig(
      final Map<String, Object> props,
      final Map<String, ?> configOverrides
  ) {
    csStatement = ConfiguredStatement.of(
        PreparedStatement.of("some sql", createSource),
        SessionConfig.of(new KsqlConfig(props), configOverrides));
  }

  private void givenSourceProps(
      final Map<String, Literal> additionalProps
  ) {
    final HashMap<String, Literal> props = new HashMap<>();
    props.put("KAFKA_TOPIC", new StringLiteral("some_topic"));
    props.putAll(additionalProps);

    when(createSource.getProperties()).thenReturn(CreateSourceProperties.from(props));
  }

  private static Object setupCopy(
      final InvocationOnMock inv,
      final CreateSource source,
      final CreateSource mock
  ) {
    final SourceName name = source.getName();
    when(mock.getName()).thenReturn(name);
    when(mock.getElements()).thenReturn(inv.getArgument(0));
    when(mock.accept(any(), any())).thenCallRealMethod();
    when(mock.getProperties()).thenReturn(inv.getArgument(1));
    return mock;
  }

}