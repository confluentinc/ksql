/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import java.io.IOException;
import org.junit.Test;

public class EntityQueryIDTest {
  final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;

  @Test
  public void shouldSerializeCorrectly() throws IOException {
    final String id = "query-id";
    final String serialized = String.format("\"%s\"", id);

    final EntityQueryId deserialized = objectMapper.readValue(serialized, EntityQueryId.class);

    assertThat(deserialized.getId(), equalTo(id));
    assertThat(objectMapper.writeValueAsString(id), equalTo(serialized));
  }
}
