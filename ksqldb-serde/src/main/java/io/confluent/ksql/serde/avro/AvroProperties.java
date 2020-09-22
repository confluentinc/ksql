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

package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.FormatProperties;
import java.util.Map;

/**
 * Properties that can be set on the Avro format.
 */
@Immutable
class AvroProperties {

  static final String AVRO_SCHEMA_NAMESPACE = "io.confluent.ksql.avro_schemas";
  static final String AVRO_SCHEMA_NAME = "KsqlDataSourceSchema";
  static final String DEFAULT_AVRO_SCHEMA_FULL_NAME =
      AVRO_SCHEMA_NAMESPACE + "." + AVRO_SCHEMA_NAME;

  static final String FULL_SCHEMA_NAME = "fullSchemaName";
  static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of(FULL_SCHEMA_NAME);

  private final ImmutableMap<String, String> properties;

  AvroProperties(final Map<String, String> formatProps) {
    this.properties = ImmutableMap.copyOf(formatProps);

    FormatProperties.validateProperties(AvroFormat.NAME, formatProps, SUPPORTED_PROPERTIES);
  }

  String getFullSchemaName() {
    return properties
        .getOrDefault(FULL_SCHEMA_NAME, DEFAULT_AVRO_SCHEMA_FULL_NAME);
  }
}
