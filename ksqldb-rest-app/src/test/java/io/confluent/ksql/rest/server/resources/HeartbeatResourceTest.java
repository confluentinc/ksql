/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HeartbeatResourceTest {

  @Mock
  private HeartbeatAgent heartbeatAgent;
  private HeartbeatResource heartbeatResource;

  @Before
  public void setUp() {
    heartbeatResource = new HeartbeatResource(heartbeatAgent);
  }

  @Test
  public void shouldSendHeartbeat() {
    // When:
    final HeartbeatMessage request = new HeartbeatMessage(new KsqlHostInfoEntity("localhost", 8080),
        1);
    final EndpointResponse response = heartbeatResource.registerHeartbeat(request);

    // Then:
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(HeartbeatResponse.class));
  }
}
