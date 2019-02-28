/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.cli.console.cmd;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.util.Event;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import javax.net.ssl.SSLException;
import javax.ws.rs.ProcessingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteServerSpecificCommandTest {

  private static final String INITIAL_SERVER_ADDRESS = "http://192.168.0.1:8080";
  private static final String VALID_SERVER_ADDRESS = "http://localhost:8088";
  private static final ServerInfo SERVER_INFO =
      new ServerInfo("1.x", "myClusterId", "myKsqlServiceId");

  @Mock
  private KsqlRestClient restClient;
  @Mock
  private Event resetCliForNewServer;

  private RemoteServerSpecificCommand command;
  private StringWriter out;
  private PrintWriter terminal;

  @Before
  public void setUp() throws Exception {
    out = new StringWriter();
    terminal = new PrintWriter(out);
    command = RemoteServerSpecificCommand.create(restClient, resetCliForNewServer);

    when(restClient.makeRootRequest()).thenReturn(RestResponse.successful(SERVER_INFO));
    when(restClient.getServerAddress()).thenReturn(new URI(INITIAL_SERVER_ADDRESS));
  }

  @Test
  public void shouldSetRestClientServerAddressWhenNonEmptyStringArg() {
    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    verify(restClient).setServerAddress(VALID_SERVER_ADDRESS);
  }

  @Test(expected = KsqlRestClientException.class)
  public void shouldThrowIfRestClientThrowsOnSet() {
    // Given:
    doThrow(new KsqlRestClientException("Boom")).when(restClient).setServerAddress("localhost:8088");

    // When:
    command.execute(ImmutableList.of("localhost:8088"), terminal);
  }

  @Test
  public void shouldPrintServerAddressWhenEmptyStringArg() {
    // When:
    command.execute(ImmutableList.of(), terminal);

    // Then:
    assertThat(out.toString(), equalTo(INITIAL_SERVER_ADDRESS + "\n"));
    verify(restClient, never()).setServerAddress(anyString());
    verify(resetCliForNewServer, never()).fire();
  }

  @Test
  public void shouldPrintErrorWhenCantConnectToNewAddress() {
    // Given:
    when(restClient.makeRootRequest()).thenThrow(
        new KsqlRestClientException("Failed to connect", new ProcessingException("Boom")));

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString("Boom"));
    assertThat(out.toString(), containsString("Failed to connect"));
  }

  @Test
  public void shouldOutputNewServerDetails() {
    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString("Server now: " + VALID_SERVER_ADDRESS));
  }

  @Test
  public void shouldPrintErrorOnErrorResponseFromRestClient() {
    // Given:
    when(restClient.makeRootRequest()).thenReturn(RestResponse.erroneous(
        Errors.ERROR_CODE_SERVER_ERROR, "it is broken"));

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString("it is broken"));
  }

  @Test
  public void shouldResetCliForNewServer() {
    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    verify(resetCliForNewServer).fire();
  }

  @Test
  public void shouldReportErrorIfFailedToGetRemoteKsqlServerInfo() {
    // Given:
    when(restClient.makeRootRequest()).thenThrow(genericConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at http://192.168.0.1:8080 does not appear to be a valid KSQL\n"
        + "server. Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldReportErrorIfRemoteKsqlServerIsUsingSSL() {
    // Given:
    when(restClient.makeRootRequest()).thenThrow(sslConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at http://192.168.0.1:8080 looks to be configured to use HTTPS /\n"
            + "SSL. Please refer to the KSQL documentation on how to configure the CLI for SSL:\n"
            + "https://docs.confluent.io/current/ksql/docs/installation/server-config/security.html"
            + "#configuring-cli-for-https"));
  }

  @Test
  public void shouldGetHelp() {
    assertThat(command.getHelpMessage(),
        containsString("server:\n\tShow the current server"));
    assertThat(command.getHelpMessage(),
        containsString("server <server>:\n\tChange the current server to <server>"));
  }

  private static Exception genericConnectionIssue() {
    return new KsqlRestClientException("failed",
        new ProcessingException("oh no"));
  }

  private static Exception sslConnectionIssue() {
    return new KsqlRestClientException("failed",
        new ProcessingException("oh no",
            new SSLException("blah")));
  }
}