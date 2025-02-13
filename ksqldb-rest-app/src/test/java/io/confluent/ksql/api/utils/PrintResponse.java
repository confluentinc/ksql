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

package io.confluent.ksql.api.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class PrintResponse {

  public final List<String> rows;
  public final JsonObject error;

  public PrintResponse(String responseBody) {
    JsonObject error = null;
    String[] parts = responseBody.split("\n");
    rows = new ArrayList<>();
    for (String part : parts) {
      if (part.startsWith("rowtime")) {
        rows.add(part);
      } else {
        assertThat(error, is(nullValue()));
        error = new JsonObject(part);
      }
    }
    this.error = error;
  }

  @Override
  public String toString() {
    return "PrintResponse{" +
        "rows=" + rows +
        ", error=" + error +
        '}';
  }
}
