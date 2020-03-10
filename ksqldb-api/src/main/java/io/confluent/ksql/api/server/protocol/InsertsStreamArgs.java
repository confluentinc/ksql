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

package io.confluent.ksql.api.server.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the arguments to an insert stream request
 */
@Immutable
public class InsertsStreamArgs {

  public final String target;
  public final boolean requiresAcks;
  public final JsonObject properties;

  public InsertsStreamArgs(final @JsonProperty(value = "target", required = true) String target,
      final @JsonProperty(value = "requiresAcks", required = true) Boolean requiresAcks,
      final @JsonProperty(value = "properties")
          Map<String, Object> properties) {
    this.target = Objects.requireNonNull(target);
    this.requiresAcks = requiresAcks;
    this.properties = properties == null ? new JsonObject() : new JsonObject(properties);
  }

  @Override
  public String toString() {
    return "InsertsStreamArgs{"
        + "target='" + target + '\''
        + ", requiresAcks=" + requiresAcks
        + ", properties=" + properties
        + '}';
  }
}
