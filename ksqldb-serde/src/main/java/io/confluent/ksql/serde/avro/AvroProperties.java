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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.serde.connect.ConnectProperties;
import java.util.Map;

/**
 * Properties that can be set on the Avro format.
 */
@Immutable
public class AvroProperties extends ConnectProperties {

  static final String AVRO_SCHEMA_NAMESPACE = "io.confluent.ksql.avro_schemas";
  static final String AVRO_SCHEMA_NAME = "KsqlDataSourceSchema";
  static final String DEFAULT_AVRO_SCHEMA_FULL_NAME =
      AVRO_SCHEMA_NAMESPACE + "." + AVRO_SCHEMA_NAME;

  static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of(
      FULL_SCHEMA_NAME,
      SCHEMA_ID
  );

  public AvroProperties(final Map<String, String> formatProps) {
    super(AvroFormat.NAME, formatProps);
  }

  @Override
  protected String getDefaultFullSchemaName() {
    return DEFAULT_AVRO_SCHEMA_FULL_NAME;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public ImmutableSet<String> getSupportedProperties() {
    return SUPPORTED_PROPERTIES;
  }
}
