/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.rest.model;

import static io.confluent.ksql.test.utils.ImmutableCollections.immutableCopyOf;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Map;

/**
 * JSON Pojo for response from rest api
 */
public final class Response {

  private final Map<String, Object> content;

  @JsonCreator
  public Response(final Map<String, Object> content) {
    this.content = immutableCopyOf(content);
  }

  public Map<String, Object> getContent() {
    return content;
  }
}
