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

package io.confluent.ksql.rest.healthcheck;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponseDetail;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import org.junit.Test;

public class HealthCheckResponseTest {
  private static final boolean IS_HEALTHY_BOOLEAN =  true;
  private static final HealthCheckResponseDetail IS_HEALTHY_DETAIL =  new HealthCheckResponseDetail(IS_HEALTHY_BOOLEAN);
  private static final String SERVER_STATE = "READY";
  @Test
  public void shouldDeserializeCorrectly() throws IOException {
    final String healthCheckResponseStr = "{" +
        "\"isHealthy\": true, " +
        "\"details\": {\"metastore\": {\"isHealthy\": true}, \"kafka\": {\"isHealthy\": true}}, " +
        "\"serverState\": \"READY\"" +
        "}";
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final HealthCheckResponse healthCheckResponse = mapper.readValue(healthCheckResponseStr, HealthCheckResponse.class);
    assertThat(healthCheckResponse.getIsHealthy(), equalTo(IS_HEALTHY_BOOLEAN));
    final HashMap<String, HealthCheckResponseDetail> details = new HashMap<>();
    details.put("metastore",IS_HEALTHY_DETAIL);
    details.put("kafka", IS_HEALTHY_DETAIL);
    assertThat(healthCheckResponse.getDetails(), equalTo(details));
    final Optional<String> serverState = Optional.of(SERVER_STATE);
    assertThat(healthCheckResponse.getServerState(), equalTo(serverState));
  }

  @Test
  public void shouldDeserializeCorrectlyWithoutServerState() throws IOException {
    final String healthCheckResponseStr = "{" +
        "\"isHealthy\": true, " +
        "\"details\": {\"metastore\": {\"isHealthy\": true}, \"kafka\": {\"isHealthy\": true}}" +
        "}";
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final HealthCheckResponse healthCheckResponse = mapper.readValue(healthCheckResponseStr, HealthCheckResponse.class);
    assertThat(healthCheckResponse.getIsHealthy(), equalTo(IS_HEALTHY_BOOLEAN));
    final HashMap<String, HealthCheckResponseDetail> details = new HashMap<>();
    details.put("metastore",IS_HEALTHY_DETAIL);
    details.put("kafka", IS_HEALTHY_DETAIL);
    assertThat(healthCheckResponse.getDetails(), equalTo(details));
    final Optional<String> serverState = Optional.empty();
    assertThat(healthCheckResponse.getServerState(), equalTo(serverState));
  }

  private void grep(final String string, final String regex) {
    assertThat(String.format("[%s] does not match [%s]", string, regex), string.matches(regex), is(true));

  }

  @Test
  public void shouldSerializeDeserializeCorrectly() throws IOException {
    final HashMap<String, HealthCheckResponseDetail> details = new HashMap<>();
    details.put("metastore", IS_HEALTHY_DETAIL);
    details.put("kafka", IS_HEALTHY_DETAIL);
    final Optional<String> serverState = Optional.of(SERVER_STATE);

    final HealthCheckResponse healthCheckResponse = new HealthCheckResponse(
        IS_HEALTHY_BOOLEAN,
        details,
        serverState
    );
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final String serialized = mapper.writeValueAsString(healthCheckResponse);
    grep(serialized, ".*\"isHealthy\":true.*");
    grep(serialized, ".*\"details\":\\{\"metastore\":\\{\"isHealthy\":true\\},\"kafka\":\\{\"isHealthy\":true\\}\\}.*");
    grep(serialized, ".*\"serverState\":\"READY\".*");
    final HealthCheckResponse deserialized = mapper.readValue(serialized, HealthCheckResponse.class);
    assertThat(deserialized, equalTo(healthCheckResponse));
  }
}
