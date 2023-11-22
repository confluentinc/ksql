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

package io.confluent.ksql.serde.json;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.serde.connect.ConnectProperties;
import java.util.Map;

public class JsonProperties extends ConnectProperties {

  static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of();

  public JsonProperties(final Map<String, String> formatProps) {
    super(JsonFormat.NAME, formatProps);
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public ImmutableSet<String> getSupportedProperties() {
    return SUPPORTED_PROPERTIES;
  }

  @Override
  public String getDefaultFullSchemaName() {
    throw new UnsupportedOperationException(
        JsonFormat.NAME + " does not implement Schema Registry support");
  }
}