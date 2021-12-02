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

package io.confluent.ksql.serde.protobuf;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.serde.connect.ConnectProperties;
import java.util.Map;

public class ProtobufProperties extends ConnectProperties {

  static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of(
      FULL_SCHEMA_NAME,
      SCHEMA_ID
  );

  static final ImmutableSet<String> INHERITABLE_PROPERTIES = ImmutableSet.of(
      FULL_SCHEMA_NAME
  );

  public ProtobufProperties(final Map<String, String> formatProps) {
    super(ProtobufFormat.NAME, formatProps);
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public ImmutableSet<String> getSupportedProperties() {
    return SUPPORTED_PROPERTIES;
  }

  @Override
  public String getDefaultFullSchemaName() {
    // Return null to be backward compatible for unset schema name
    return null;
  }
}
