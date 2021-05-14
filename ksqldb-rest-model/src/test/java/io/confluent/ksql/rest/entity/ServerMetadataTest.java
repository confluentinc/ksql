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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.ServerClusterId.Scope;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerMetadataTest {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private static final String JSON = "{"
      + "\"version\":\"1.0.0\","
      + "\"clusterId\":"
      + "{"
      + "\"scope\":"
      + "{"
      + "\"path\":[\"p1\",\"p2\"],"
      + "\"clusters\":{\"kafka-cluster\":\"kafka1\",\"ksql-cluster\":\"ksql1\"}"
      + "},"
      + "\"id\":\"theId\""
      + "}"
      + "}";

  private static final ImmutableMap<String, String> CLUSTERS = ImmutableMap.of(
      "kafka-cluster", "kafka1",
      "ksql-cluster", "ksql1"
  );

  private static final ImmutableList<String> PATH = ImmutableList.of("p1", "p2");
  private static final String ID = "theId";
  private static final String VERSION = "1.0.0";

  @Mock
  private ServerClusterId serverClusterId;
  @Mock
  private Scope scope;

  @Test
  public void shouldSerializeToJson() throws IOException {
    // Given:
    when(serverClusterId.getId()).thenReturn(ID);
    when(serverClusterId.getScope()).thenReturn(scope);
    when(scope.getPath()).thenReturn(PATH);
    when(scope.getClusters()).thenReturn(CLUSTERS);

    final ServerMetadata expected = new ServerMetadata(VERSION, serverClusterId);

    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(expected);

    // Then:
    assertThat(json, is(JSON));
  }

  @Test
  public void shouldDeserializeFromJson() throws IOException {
    final ServerMetadata actual = OBJECT_MAPPER.readValue(JSON, ServerMetadata.class);

    // Then:
    assertThat(actual.getVersion(), is(VERSION));
    assertThat(actual.getClusterId().getId(), is(ID));
    assertThat(actual.getClusterId().getScope().getPath(), is(PATH));
    assertThat(actual.getClusterId().getScope().getClusters(), is(CLUSTERS));
  }
}
