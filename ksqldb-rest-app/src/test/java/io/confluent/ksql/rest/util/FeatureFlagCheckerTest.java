/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FeatureFlagCheckerTest {
  @Test
  public void shouldThrowOnCreateStreamWithHeadersIfFeatureFlagIsDisabled() {
    // Given
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, false
    ));

    final CreateStream createStream = new CreateStream(
        SourceName.of("stream1"),
        TableElements.of(new TableElement(
            ColumnName.of("col1"),
            new Type(SqlArray.of(
                SqlStruct.builder()
                    .field("KEY", SqlTypes.STRING)
                    .field("VALUE", SqlTypes.BYTES).build())),
            new ColumnConstraints.Builder().headers().build()
        )),
        false,
        false,
        CreateSourceProperties.from(ImmutableMap.of(
            CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic1")
        )),
        false
    );
    final ConfiguredStatement<?> configuredStatement = configured(config, createStream);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> FeatureFlagChecker.throwOnDisabledFeatures(configuredStatement)
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Cannot create Stream because schema with headers columns is disabled."));
  }

  @Test
  public void shouldNotThrowOnCreateStreamWithHeadersIfFeatureFlagIsEnabled() {
    // Given
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true
    ));

    final CreateStream createStream = new CreateStream(
        SourceName.of("stream1"),
        TableElements.of(new TableElement(
            ColumnName.of("col1"),
            new Type(SqlTypes.INTEGER),
            new ColumnConstraints.Builder().headers().build()
        )),
        false,
        false,
        CreateSourceProperties.from(ImmutableMap.of(
            CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic1")
        )),
        false
    );
    final ConfiguredStatement<?> configuredStatement = configured(config, createStream);

    // When/Then
    FeatureFlagChecker.throwOnDisabledFeatures(configuredStatement);
  }

  @SuppressWarnings("rawtypes")
  private ConfiguredStatement<?> configured(final KsqlConfig config, final Statement statement) {
    final ConfiguredStatement mockConfigured = mock(ConfiguredStatement.class);

    when(mockConfigured.getStatement())
        .thenReturn(statement);
    when(mockConfigured.getSessionConfig())
        .thenReturn(SessionConfig.of(config, ImmutableMap.of()));

    return mockConfigured;
  }
}
