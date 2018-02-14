/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import org.easymock.EasyMock;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertTrue;

public class KsqlRestClientTest {

  @Test
  public void shouldRaiseAuthenticationExceptionOn401Response() {
    String serverAddress = "http://foobar";
    Client client = mockClientExpectingGetRequestAndReturningStatus(serverAddress, Response.Status.UNAUTHORIZED);
    KsqlRestClient restClient = new KsqlRestClient(client, serverAddress);
    RestResponse restResponse = restClient.getServerInfo();
    assertTrue(restResponse.isErroneous());
  }

  @Test
  public void shouldReturnSuccessfulResponseWhenAuthenticationSucceeds() {
    String serverAddress = "http://foobar";
    Client client = mockClientExpectingGetRequestAndReturningStatus(serverAddress, Response.Status.OK);
    KsqlRestClient restClient = new KsqlRestClient(client, serverAddress);
    RestResponse restResponse = restClient.getServerInfo();
    assertTrue(restResponse.isSuccessful());
  }

  private Client mockClientExpectingGetRequestAndReturningStatus(String server, Response.Status status) {
    Client client = EasyMock.createNiceMock(Client.class);

    WebTarget target = EasyMock.createNiceMock(WebTarget.class);
    EasyMock.expect(client.target(server)).andReturn(target);
    EasyMock.expect(target.path("/info")).andReturn(target);
    Invocation.Builder builder = EasyMock.createNiceMock(Invocation.Builder.class);
    EasyMock.expect(target.request(MediaType.APPLICATION_JSON_TYPE)).andReturn(builder);
    Response response = EasyMock.createNiceMock(Response.class);
    EasyMock.expect(builder.get()).andReturn(response);

    EasyMock.expect(response.getStatus()).andReturn(status.getStatusCode());

    EasyMock.replay(client, target, builder, response);
    return client;
  }
}
