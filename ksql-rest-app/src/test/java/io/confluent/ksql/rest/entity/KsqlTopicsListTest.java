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

package io.confluent.ksql.rest.entity;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import org.junit.Test;


public class KsqlTopicsListTest {
  @Test
  public void testSerde() throws Exception {
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final KsqlTopicsList expected = new KsqlTopicsList(
        "SHOW TOPICS;",
        ImmutableList.of(new KsqlTopicInfo("ksqltopic", "kafkatopic", DataSourceSerDe.JSON))
    );
    final String json = mapper.writeValueAsString(expected);
    assertEquals(
        "{\"@type\":\"ksql_topics\",\"statementText\":\"SHOW TOPICS;\"," +
        "\"topics\":[{\"name\":\"ksqltopic\",\"kafkaTopic\":\"kafkatopic\",\"format\":\"JSON\"}]}",
        json);

    final KsqlTopicsList actual = mapper.readValue(json, KsqlTopicsList.class);
    assertEquals(expected, actual);
  }
}
