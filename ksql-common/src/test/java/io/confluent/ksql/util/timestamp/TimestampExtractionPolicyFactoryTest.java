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

package io.confluent.ksql.util.timestamp;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TimestampExtractionPolicyFactoryTest {


  private final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
      .field("id", Schema.OPTIONAL_INT64_SCHEMA);

  private KsqlConfig ksqlConfig;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
            LogicalSchema.of(schemaBuilder.build()),
            Optional.empty(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    assertThat(result.create(0), instanceOf(FailOnInvalidTimestamp.class));
  }

  @Test
  public void shouldThrowIfTimestampExtractorConfigIsInvalidClass() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        this.getClass()
    ));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "cannot be cast to org.apache.kafka.streams.processor.TimestampExtractor");

    // When:
    TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            LogicalSchema.of(schemaBuilder.build()),
            Optional.empty(),
            Optional.empty()
        );
  }

  @Test
  public void shouldCreateMetadataPolicyWithDefaultFailedOnInvalidTimestamp() {
    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            LogicalSchema.of(schemaBuilder.build()),
            Optional.empty(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    assertThat(result.create(0), instanceOf(FailOnInvalidTimestamp.class));
  }

  @Test
  public void shouldCreateMetadataPolicyWithConfiguredUsePreviousTimeOnInvalidTimestamp() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UsePreviousTimeOnInvalidTimestamp.class
    ));

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            LogicalSchema.of(schemaBuilder.build()),
            Optional.empty(),
            Optional.empty()
        );

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
    assertThat(result.create(0), instanceOf(UsePreviousTimeOnInvalidTimestamp.class));
  }

  @Test
  public void shouldCreateLongTimestampPolicyWhenTimestampFieldIsOfTypeLong() {
    // Given:
    final String timestamp = "timestamp";
    final Schema schema = schemaBuilder
        .field(timestamp.toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(ksqlConfig, LogicalSchema.of(schema), Optional.of(timestamp), Optional.empty());

    // Then:
    assertThat(result, instanceOf(LongColumnTimestampExtractionPolicy.class));
    assertThat(result.timestampField(), equalTo(timestamp.toUpperCase()));
  }

  @Test
  public void shouldFailIfCantFindTimestampField() {
    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    TimestampExtractionPolicyFactory
        .create(
            ksqlConfig,
            LogicalSchema.of(schemaBuilder.build()),
            Optional.of("whateva"),
            Optional.empty()
        );
  }

  @Test
  public void shouldCreateStringTimestampPolicyWhenTimestampFieldIsStringTypeAndFormatProvided() {
    // Given:
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(ksqlConfig, LogicalSchema.of(schema), Optional.of(field), Optional.of("yyyy-MM-DD"));

    // Then:
    assertThat(result, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(result.timestampField(), equalTo(field.toUpperCase()));
  }

  @Test
  public void shouldFailIfStringTimestampTypeAndFormatNotSupplied() {
    // Given:
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    TimestampExtractionPolicyFactory
        .create(ksqlConfig, LogicalSchema.of(schema), Optional.of(field), Optional.empty());
  }

  @Test
  public void shouldThorwIfLongTimestampTypeAndFormatIsSupplied() {
    // Given:
    final String timestamp = "timestamp";
    final Schema schema = schemaBuilder
        .field(timestamp.toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    TimestampExtractionPolicyFactory
        .create(ksqlConfig, LogicalSchema.of(schema), Optional.of(timestamp), Optional.of("b"));
  }

  @Test
  public void shouldThrowIfTimestampFieldTypeIsNotLongOrString() {
    // Given:
    final String field = "blah";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    TimestampExtractionPolicyFactory
        .create(ksqlConfig, LogicalSchema.of(schema), Optional.of(field), Optional.empty());
  }
}