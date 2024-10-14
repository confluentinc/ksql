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

package io.confluent.ksql.execution.streams.timestamp;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimestampExtractionPolicyFactoryTest {

  private final LogicalSchema.Builder schemaBuilder2 = LogicalSchema.builder()
      .valueColumn(ColumnName.of("id"), SqlTypes.BIGINT);

  private KsqlConfig ksqlConfig;

  @Mock
  private ProcessingLogger logger;

  @Before
  public void setup() {
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
  }

  @Test
  public void shouldCreateMetadataPolicyWhenTimestampFieldNotProvided() {
    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schemaBuilder2.build(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    final TimestampExtractor timestampExtractor = result.create(Optional.empty(), true, logger);
    assertThat(timestampExtractor, instanceOf(LoggingTimestampExtractor.class));
    assertThat(
        ((LoggingTimestampExtractor) timestampExtractor).getDelegate(),
        instanceOf(MetadataTimestampExtractor.class));
  }

  @Test
  public void shouldThrowIfTimestampExtractorConfigIsInvalidClass() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.getClass()
    ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> TimestampExtractionPolicyFactory
            .create(
                ksqlConfig,
                schemaBuilder2.build(),
                Optional.empty()
            )
    );

    // Then:
    // split the assertion into two different checks to support the error message differences
    // between JDK8 and JDK17
    assertThat(e.getMessage(), containsString("cannot be cast"));
    assertThat(e.getMessage(), containsString(
        "org.apache.kafka.streams.processor.TimestampExtractor"));
  }

  @Test
  public void shouldCreateMetadataPolicyWithDefaultFailedOnInvalidTimestamp() {
    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schemaBuilder2.build(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    assertThat(((MetadataTimestampExtractor) result.create(Optional.empty())).getTimestampExtractor(),
        instanceOf(FailOnInvalidTimestamp.class));
  }

  @Test
  public void shouldCreateMetadataPolicyWithConfiguredUsePartitionTimeOnInvalidTimestamp() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UsePartitionTimeOnInvalidTimestamp.class
    ));

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schemaBuilder2.build(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    assertThat(((MetadataTimestampExtractor) result.create(Optional.empty())).getTimestampExtractor(),
        instanceOf(UsePartitionTimeOnInvalidTimestamp.class));
  }

  @Test
  public void shouldCreateLongTimestampPolicyWhenTimestampFieldIsOfTypeLong() {
    // Given:
    final String timestamp = "timestamp";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(timestamp.toUpperCase()), SqlTypes.BIGINT)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schema,
            Optional.of(
                new TimestampColumn(
                    ColumnName.of(timestamp.toUpperCase()),
                    Optional.empty()
                )
            )
        );

    // Then:
    assertThat(result, instanceOf(LongColumnTimestampExtractionPolicy.class));
    assertThat(result.getTimestampField(),
        equalTo(ColumnName.of(timestamp.toUpperCase())));
  }

  @Test
  public void shouldCreateTimestampPolicyWhenTimestampFieldIsOfTypeTimestamp() {
    // Given:
    final String timestamp = "timestamp";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(timestamp.toUpperCase()), SqlTypes.TIMESTAMP)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schema,
            Optional.of(
                new TimestampColumn(
                    ColumnName.of(timestamp.toUpperCase()),
                    Optional.empty()
                )
            )
        );

    // Then:
    assertThat(result, instanceOf(TimestampColumnTimestampExtractionPolicy.class));
    assertThat(result.getTimestampField(),
        equalTo(ColumnName.of(timestamp.toUpperCase())));
  }

  @Test
  public void shouldFailIfCantFindTimestampField() {
    // When:
    assertThrows(
        KsqlException.class,
        () -> TimestampExtractionPolicyFactory
            .create(
                ksqlConfig,
                schemaBuilder2.build(),
                Optional.of(
                    new TimestampColumn(
                        ColumnName.of("whateva"),
                        Optional.empty()
                    )
                )
            )
    );
  }

  @Test
  public void shouldCreateStringTimestampPolicyWhenTimestampFieldIsStringTypeAndFormatProvided() {
    // Given:
    final String field = "my_string_field";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(field.toUpperCase()), SqlTypes.STRING)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            schema,
            Optional.of(
                new TimestampColumn(
                    ColumnName.of(field.toUpperCase()),
                    Optional.of("yyyy-MM-DD")
                )
            )
        );

    // Then:
    assertThat(result, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(result.getTimestampField(),
        equalTo(ColumnName.of(field.toUpperCase())));
  }

  @Test
  public void shouldFailIfStringTimestampTypeAndFormatNotSupplied() {
    // Given:
    final String field = "my_string_field";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(field.toUpperCase()), SqlTypes.STRING)
        .build();

    // When:
    assertThrows(
        KsqlException.class,
        () -> TimestampExtractionPolicyFactory
            .create(
                ksqlConfig,
                schema,
                Optional.of(
                    new TimestampColumn(
                        ColumnName.of(field.toUpperCase()),
                        Optional.empty()
                    )
                )
            )
    );
  }

  @Test
  public void shouldThrowIfLongTimestampTypeAndFormatIsSupplied() {
    // Given:
    final String timestamp = "timestamp";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(timestamp.toUpperCase()), SqlTypes.BIGINT)
        .build();

    // When:
    assertThrows(
        KsqlException.class,
        () -> TimestampExtractionPolicyFactory
            .create(ksqlConfig,
                schema,
                Optional.of(
                    new TimestampColumn(
                        ColumnName.of(timestamp.toUpperCase()),
                        Optional.of("b")
                    )
                )
            )
    );
  }

  @Test
  public void shouldThrowIfTimestampTypeAndFormatIsSupplied() {
    // Given:
    final String timestamp = "timestamp";
    final LogicalSchema schema = schemaBuilder2
        .valueColumn(ColumnName.of(timestamp.toUpperCase()), SqlTypes.TIMESTAMP)
        .build();

    // When:
    assertThrows(
        KsqlException.class,
        () -> TimestampExtractionPolicyFactory
            .create(ksqlConfig,
                schema,
                Optional.of(
                    new TimestampColumn(
                        ColumnName.of(timestamp.toUpperCase()),
                        Optional.of("b")
                    )
                )
            )
    );
  }

  @Test
  public void shouldThrowIfTimestampFieldTypeIsNotLongOrTimestampOrString() {

    final Set<SqlPrimitiveType> allowedTypes = Sets.newHashSet(
        SqlTypes.BIGINT,
        SqlTypes.STRING,
        SqlTypes.TIMESTAMP);

    for (SqlPrimitiveType sqlType : SqlTypes.ALL) {
      if (allowedTypes.contains(sqlType)) {
        continue;
      }

      // Given:
      final String field = "blah_" + sqlType.toString();
      final LogicalSchema schema = schemaBuilder2
          .valueColumn(ColumnName.of(field.toUpperCase()), sqlType)
          .build();

      // When:
      assertThrows(
          KsqlException.class,
          () -> TimestampExtractionPolicyFactory
              .create(ksqlConfig,
                  schema,
                  Optional.of(
                      new TimestampColumn(
                          ColumnName.of(field),
                          Optional.empty()
                      )
                  )
              )
      );
    }
  }
}
