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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.util.Event;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import javax.net.ssl.SSLException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteServerSpecificCommandTest {

  private static final String INITIAL_SERVER_ADDRESS = "http://192.168.0.1:8080";
  private static final String VALID_SERVER_ADDRESS = "http://localhost:8088";
  private final ServerInfo serverInfo = mock(ServerInfo.class);

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

    when(serverInfo.getVersion()).thenReturn("100.0.0");

    when(restClient.getServerInfo()).thenReturn(RestResponse.successful(OK.code(), serverInfo));
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
    assertThat(out.toString(), equalTo(INITIAL_SERVER_ADDRESS + System.lineSeparator()));
    verify(restClient, never()).setServerAddress(anyString());
    verify(resetCliForNewServer, never()).fire();
  }

  @Test
  public void shouldPrintErrorWhenCantConnectToNewAddress() {
    // Given:
    when(restClient.getServerInfo()).thenThrow(
        new KsqlRestClientException("Failed to connect", new IOException("Boom")));

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
    when(restClient.getServerInfo()).thenReturn(RestResponse.erroneous(
        INTERNAL_SERVER_ERROR.code(), "it is broken"));

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
  public void shouldFailOnUnsupportedServerVersion() {
    // Given:
    //reset(serverInfo);
    when(serverInfo.getVersion()).thenReturn("5.3.0");

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    System.out.println(out.toString());
    assertThat(out.toString(), containsString("ERROR: Server version not supported."));
  }

  @Test
  public void shouldReportErrorIfFailedToGetRemoteKsqlServerInfo() {
    // Given:
    when(restClient.getServerInfo()).thenThrow(genericConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at http://192.168.0.1:8080 does not appear to be a valid KSQL"
            + System.lineSeparator()
        + "server. Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldReportErrorIfRemoteKsqlServerIsUsingSSL() {
    // Given:
    when(restClient.getServerInfo()).thenThrow(sslConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at http://192.168.0.1:8080 looks to be configured to use HTTPS /"
            + System.lineSeparator()
            + "SSL. Please refer to the KSQL documentation on how to configure the CLI for SSL:"
            + System.lineSeparator()
            + DocumentationLinks.SECURITY_CLI_SSL_DOC_URL));
  }

  @Test
  public void shouldGetHelp() {
    assertThat(command.getHelpMessage(),
        containsString("server:" + System.lineSeparator() + "\tShow the current server"));
    assertThat(command.getHelpMessage(),
        containsString("server <server>:" + System.lineSeparator()
            + "\tChange the current server to <server>"));
  }

  private static Exception genericConnectionIssue() {
    return new KsqlRestClientException("failed",
        new IOException("oh no"));
  }

  private static Exception sslConnectionIssue() {
    return new KsqlRestClientException("failed",
        new IOException("oh no",
            new SSLException("blah")));
  }
}