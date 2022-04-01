/*
 * Copyright 2022 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.FormatProperties;
import java.util.Map;

@Immutable
public class ProtobufProperties {

  public static final String UNWRAP_PRIMITIVES = "unwrapPrimitives";
  public static final String UNWRAP = "true";
  private static final String WRAP = "false";

  static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of(UNWRAP_PRIMITIVES);

  private final ImmutableMap<String, String> properties;

  public ProtobufProperties(final Map<String, String> formatProps) {
    this.properties = ImmutableMap.copyOf(formatProps);

    FormatProperties.validateProperties(ProtobufFormat.NAME, formatProps, SUPPORTED_PROPERTIES);
  }

  public boolean getUnwrapPrimitives() {
    return UNWRAP.equalsIgnoreCase(properties.getOrDefault(UNWRAP_PRIMITIVES, WRAP));
  }
}
