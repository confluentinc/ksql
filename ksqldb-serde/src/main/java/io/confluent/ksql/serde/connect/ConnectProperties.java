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

package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.FormatProperties;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for properties that are common for Connect Format that support Schema Registry
 */
@Immutable
public abstract class ConnectProperties {
  public static final String FULL_SCHEMA_NAME = "fullSchemaName";
  public static final String SCHEMA_ID = "schemaId";
  public static final String SUBJECT_NAME = "subjectName";

  protected final ImmutableMap<String, String> properties;

  public ConnectProperties(final String formatName, final Map<String, String> formatProps) {
    this.properties = ImmutableMap.copyOf(formatProps);

    FormatProperties.validateProperties(formatName, formatProps, getSupportedProperties());
  }

  public abstract ImmutableSet<String> getSupportedProperties();

  protected abstract String getDefaultFullSchemaName();

  public String getFullSchemaName() {
    return properties.getOrDefault(FULL_SCHEMA_NAME, getDefaultFullSchemaName());
  }

  public Optional<Integer> getSchemaId() {
    final String schemaId = properties.get(SCHEMA_ID);
    return schemaId == null ? Optional.empty() : Optional.of(Integer.parseInt(schemaId));
  }

  public Optional<String> getSubjectName() {
    return Optional.ofNullable(properties.get(SUBJECT_NAME));
  }
}