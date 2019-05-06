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

import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class TimestampExtractionPolicyFactoryTest {


  private final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
      .field("id", Schema.OPTIONAL_INT64_SCHEMA);

  @Test
  public void shouldCreateMetadataPolicyWhenTimestampFieldNotProvided() {
    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(KsqlSchema.of(schemaBuilder.build()), null, null);

    // Then:
    assertThat(result, instanceOf(MetadataTimestampExtractionPolicy.class));
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
        .create(KsqlSchema.of(schema), timestamp, null);

    // Then:
    assertThat(result, instanceOf(LongColumnTimestampExtractionPolicy.class));
    assertThat(result.timestampField(), equalTo(timestamp.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfCantFindTimestampField() {
    TimestampExtractionPolicyFactory
        .create(KsqlSchema.of(schemaBuilder.build()), "whateva", null);
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
        .create(KsqlSchema.of(schema), field, "yyyy-MM-DD");

    // Then:
    assertThat(result, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(result.timestampField(), equalTo(field.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfStringTimestampTypeAndFormatNotSupplied() {
    // Given:
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // When:
    TimestampExtractionPolicyFactory
        .create(KsqlSchema.of(schema), field, null);
  }

  @Test
  public void shouldSupportFieldsWithQuotedStrings() {
    // Given:
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // When:
    final TimestampExtractionPolicy result = TimestampExtractionPolicyFactory
        .create(KsqlSchema.of(schema), "'" + field + "'", "'yyyy-MM-DD'");

    // Then:
    assertThat(result, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(result.timestampField(), equalTo(field.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfTimestampFieldTypeIsNotLongOrString() {
    // Given:
    final String field = "blah";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    // When:
    TimestampExtractionPolicyFactory
        .create(KsqlSchema.of(schema), "'" + field + "'", null);
  }

}