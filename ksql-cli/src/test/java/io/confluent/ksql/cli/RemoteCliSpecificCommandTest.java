/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.cli;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.resources.Errors;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.Collections;
import javax.ws.rs.ProcessingException;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class RemoteCliSpecificCommandTest {

  private static final String INITIAL_SERVER_ADDRESS = "http://192.168.0.1:8080";
  private static final String VALID_SERVER_ADDRESS = "http://localhost:8088";
  private static final ServerInfo SERVER_INFO =
      new ServerInfo("1.x", "myClusterId", "myKsqlServiceId");

  @Mock(MockType.NICE)
  private KsqlRestClient restClient;
  private Cli.RemoteServerSpecificCommand command;
  private StringWriter out;

  @Before
  public void setUp() {
    out = new StringWriter();
    command = new Cli.RemoteServerSpecificCommand(restClient, new PrintWriter(out));
  }

  @Test
  public void shouldRestClientServerAddressWhenNonEmptyStringArg() {
    expect(restClient.makeRootRequest()).andReturn(RestResponse.successful(SERVER_INFO));
    restClient.setServerAddress(VALID_SERVER_ADDRESS);
    expectLastCall();
    replay(restClient);

    command.execute(VALID_SERVER_ADDRESS);

    verify(restClient);
  }

  @Test(expected = KsqlRestClientException.class)
  public void shouldThrowIfRestClientThrowsOnSet() {
    restClient.setServerAddress("localhost:8088");
    expectLastCall().andThrow(new KsqlRestClientException("Boom"));
    replay(restClient);

    command.execute("localhost:8088");
  }

  @Test
  public void shouldPrintServerAddressWhenEmptyStringArg() throws Exception {
    expect(restClient.makeRootRequest()).andReturn(RestResponse.successful(SERVER_INFO));
    expect(restClient.getServerAddress()).andReturn(new URI(INITIAL_SERVER_ADDRESS));
    restClient.setServerAddress(anyString());
    expectLastCall().andThrow(new AssertionError("should not set address"));
    replay(restClient);

    command.execute("");

    assertThat(out.toString(), equalTo(INITIAL_SERVER_ADDRESS + "\n"));
  }

  @Test
  public void shouldPrintErrorWhenCantConnectToNewAddress() {
    expect(restClient.makeRootRequest()).andThrow(
        new KsqlRestClientException("Failed to connect", new ProcessingException("Boom")));
    replay(restClient);

    command.execute(VALID_SERVER_ADDRESS);

    assertThat(out.toString(), containsString("Boom"));
    assertThat(out.toString(), containsString("Failed to connect"));
  }

  @Test
  public void shouldOutputNewServerDetails() {
    expect(restClient.makeRootRequest()).andReturn(RestResponse.successful(SERVER_INFO));
    replay(restClient);

    command.execute(VALID_SERVER_ADDRESS);

    assertThat(out.toString(), containsString("Server now: " + VALID_SERVER_ADDRESS));
  }

  @Test
  public void shouldPrintErrorOnErrorResponseFromRestClient() {
    final Cli.RemoteServerSpecificCommand command = new Cli.RemoteServerSpecificCommand(
        new KsqlRestClient(INITIAL_SERVER_ADDRESS, Collections.emptyMap()) {
          @Override
          public RestResponse<ServerInfo> getServerInfo() {
            return RestResponse.erroneous(
                Errors.ERROR_CODE_SERVER_ERROR, "it is broken");
          }
        }, new PrintWriter(out));
    command.execute(VALID_SERVER_ADDRESS);

    assertThat(out.toString(), containsString("it is broken"));
  }

  @Test
  public void shouldPrintHelp() {
    command.printHelp();
    assertThat(out.toString(), containsString("server:\n\tShow the current server"));
    assertThat(out.toString(), containsString("server <server>:\n\tChange the current server to <server>"));
  }

}