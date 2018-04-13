/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util.timestamp;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import io.confluent.ksql.util.KsqlException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TimestampExtractionPolicyFactoryTest {


  private final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
      .field("id", Schema.INT64_SCHEMA);

  @Test
  public void shouldCreateMetadataPolicyWhenTimestampFieldNotProvided() {
    assertThat(TimestampExtractionPolicyFactory.create(schemaBuilder.build(), null, null),
        instanceOf(MetadataTimestampExtractionPolicy.class));
  }

  @Test
  public void shouldCreateLongTimestampPolicyWhenTimestampFieldIsOfTypeLong() {
    final String timestamp = "timestamp";
    final Schema schema = schemaBuilder
        .field(timestamp.toUpperCase(), Schema.INT64_SCHEMA)
        .build();
    TimestampExtractionPolicy extractionPolicy
        = TimestampExtractionPolicyFactory.create(schema, timestamp, null);
    assertThat(extractionPolicy, instanceOf(LongColumnTimestampExtractionPolicy.class));
    assertThat(extractionPolicy.timestampField(), equalTo(timestamp.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfCantFindTimestampField() {
    TimestampExtractionPolicyFactory.create(schemaBuilder.build(), "whateva", null);
  }

  @Test
  public void shouldCreateStringTimestampPolicyWhenTimestampFieldIsStringTypeAndFormatProvided() {
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.STRING_SCHEMA)
        .build();
    TimestampExtractionPolicy extractionPolicy
        = TimestampExtractionPolicyFactory.create(schema, field, "yyyy-MM-DD");
    assertThat(extractionPolicy, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(extractionPolicy.timestampField(), equalTo(field.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfStringTimestampTypeAndFormatNotSupplied() {
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.STRING_SCHEMA)
        .build();

    TimestampExtractionPolicyFactory.create(schema, field, null);
  }

  @Test
  public void shouldSupportFieldsWithQuotedStrings() {
    final String field = "my_string_field";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.STRING_SCHEMA)
        .build();
    TimestampExtractionPolicy extractionPolicy
        = TimestampExtractionPolicyFactory.create(schema, "'"+ field+ "'", "'yyyy-MM-DD'");
    assertThat(extractionPolicy, instanceOf(StringTimestampExtractionPolicy.class));
    assertThat(extractionPolicy.timestampField(), equalTo(field.toUpperCase()));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfTimestampFieldTypeIsNotLongOrString() {
    final String field = "blah";
    final Schema schema = schemaBuilder
        .field(field.toUpperCase(), Schema.FLOAT64_SCHEMA)
        .build();
    TimestampExtractionPolicyFactory.create(schema, "'"+ field+ "'", null);
  }

}