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

package io.confluent.ksql.api.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class QueryResponse {

  public final JsonObject responseObject;
  public final List<JsonArray> rows;
  public final JsonObject error;

  public QueryResponse(String responseBody) {
    JsonObject error = null;
    String[] parts = responseBody.split("\n");
    responseObject = new JsonObject(parts[0]);
    rows = new ArrayList<>();
    for (int i = 1; i < parts.length; i++) {
      if (parts[i].startsWith("[")) {
        JsonArray row = new JsonArray(parts[i]);
        rows.add(row);
      } else {
        assertThat(error, is(nullValue()));
        error = new JsonObject(parts[i]);
      }
    }
    this.error = error;
  }

  @Override
  public String toString() {
    return "QueryResponse{" +
        "metadata=" + responseObject +
        ", rows=" + rows +
        ", error=" + error +
        '}';
  }
}
