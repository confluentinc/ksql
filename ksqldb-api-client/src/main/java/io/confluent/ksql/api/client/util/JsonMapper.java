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

package io.confluent.ksql.api.client.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableList;
import io.vertx.core.json.jackson.DatabindCodec;

public final class JsonMapper {

  private static final ObjectMapper MAPPER = DatabindCodec.mapper();

  static {
    MAPPER.registerModule(new GuavaModule());
    MAPPER.registerSubtypes(ImmutableList.class);
  }

  private JsonMapper() {
  }

  public static ObjectMapper get() {
    return MAPPER;
  }
}