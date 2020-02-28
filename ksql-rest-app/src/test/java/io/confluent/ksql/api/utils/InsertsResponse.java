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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class InsertsResponse {

  public final List<JsonObject> acks;
  public final JsonObject error;

  public InsertsResponse(String responseBody) {
    String[] parts = responseBody.split("\n");
    acks = new ArrayList<>();
    JsonObject error = null;
    for (int i = 0; i < parts.length; i++) {
      JsonObject jsonObject = new JsonObject(parts[i]);
      String status = jsonObject.getString("status");
      assertThat(status, is(notNullValue()));
      if (status.equals("ok")) {
        acks.add(jsonObject);
      } else {
        assertThat(error, is(nullValue()));
        error = jsonObject;
      }
    }
    this.error = error;
  }

  @Override
  public String toString() {
    return "QueryResponse{" +
        "acks=" + acks +
        '}';
  }
}
