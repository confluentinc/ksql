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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
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
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteServerSpecificCommandTest {

  private static final String INITIAL_SERVER_ADDRESS = "http://192.168.0.1:8080";
  private static final String VALID_SERVER_ADDRESS = "http://localhost:8088";
  private static final String CCLOUD_SERVER_ADDRESS =
      "https://pksqlc-abcde.us-west-2.aws.confluent.cloud:443";

  private static final String KSQL_SERVICE_ID = "default_";
  private static final String CCLOUD_SERVICE_ID = "pksqlc-abcde";

  @Mock
  private KsqlRestClient restClient;
  @Mock
  private Event resetCliForNewServer;
  @Mock
  private ServerInfo serverInfo;
  @Mock
  private ServerInfo ccloudServerInfo;

  private RemoteServerSpecificCommand command;
  private StringWriter out;
  private PrintWriter terminal;
  private String serverAddress;

  @Before
  public void setUp() {
    out = new StringWriter();
    terminal = new PrintWriter(out);
    command = RemoteServerSpecificCommand.create(restClient, resetCliForNewServer);

    givenServerAddressHandling();
    givenServerInfoHandling();

    when(serverInfo.getKsqlServiceId()).thenReturn(KSQL_SERVICE_ID);
    when(ccloudServerInfo.getKsqlServiceId()).thenReturn(CCLOUD_SERVICE_ID);
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
    reset(restClient);
    givenServerAddressHandling();
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
  public void shouldReportErrorIfFailedToGetRemoteKsqlServerInfo() {
    // Given:
    reset(restClient);
    givenServerAddressHandling();
    when(restClient.getServerInfo()).thenThrow(genericConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at " + VALID_SERVER_ADDRESS + " does not appear to be a valid KSQL"
            + System.lineSeparator()
        + "server. Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldReportErrorIfRemoteKsqlServerIsUsingSSL() {
    // Given:
    reset(restClient);
    givenServerAddressHandling();
    when(restClient.getServerInfo()).thenThrow(sslConnectionIssue());

    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    assertThat(out.toString(), containsString(
        "Remote server at " + VALID_SERVER_ADDRESS + " looks to be configured to use HTTPS /"
            + System.lineSeparator()
            + "SSL. Please refer to the KSQL documentation on how to configure the CLI for SSL:"
            + System.lineSeparator()
            + DocumentationLinks.SECURITY_CLI_SSL_DOC_URL));
  }

  @Test
  public void shouldIdentifyNonCCloudServer() {
    // When:
    command.execute(ImmutableList.of(VALID_SERVER_ADDRESS), terminal);

    // Then:
    verify(restClient).setIsCCloudServer(false);
  }

  @Test
  public void shouldIdentifyCCloudServer() {
    // When:
    command.execute(ImmutableList.of(CCLOUD_SERVER_ADDRESS), terminal);

    // Then:
    verify(restClient).setIsCCloudServer(true);
  }

  @Test
  public void shouldGetHelp() {
    assertThat(command.getHelpMessage(),
        containsString("server:" + System.lineSeparator() + "\tShow the current server"));
    assertThat(command.getHelpMessage(),
        containsString("server <server>:" + System.lineSeparator()
            + "\tChange the current server to <server>"));
  }

  private void givenServerAddressHandling() {
    serverAddress = INITIAL_SERVER_ADDRESS;
    doAnswer(invocation -> {
      serverAddress = invocation.getArgument(0);
      return null;
    }).when(restClient).setServerAddress(anyString());
    when(restClient.getServerAddress()).thenAnswer(invocation -> new URI(serverAddress));
  }

  private void givenServerInfoHandling() {
    when(restClient.getServerInfo()).thenAnswer(invocation -> {
      if (serverAddress.equals(VALID_SERVER_ADDRESS)) {
        return RestResponse.successful(OK.code(), serverInfo);
      } else if (serverAddress.equals(CCLOUD_SERVER_ADDRESS)) {
        return RestResponse.successful(OK.code(), ccloudServerInfo);
      } else {
        // errors if during execution, but fine during additional mocking
        return null;
      }
    });
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