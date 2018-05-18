/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.GenericRow;

public class JsonUtil {

  public GenericRow buildGenericRowFromJson(String jsonString) throws IOException {
    JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
    List columns = new ArrayList();
    if (jsonNode.has("row") && jsonNode.has("errorMessage")) {
      if (jsonNode.get("errorMessage").toString().equalsIgnoreCase("null")) {
        JsonNode columnsArray = jsonNode.get("row").get("columns");
        for (JsonNode field: columnsArray) {
          columns.add(field.asText());
        }
        return new GenericRow(columns);
      } else {
        return null;
      }
    } else {
      Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
      while (fields.hasNext()) {
        columns.add(fields.next().getValue());
      }
      return new GenericRow(columns);
    }
  }

}