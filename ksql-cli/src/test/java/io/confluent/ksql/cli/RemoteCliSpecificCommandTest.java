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

import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;

import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ServerInfo;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class RemoteCliSpecificCommandTest {

  private final StringWriter out = new StringWriter();
  private final KsqlRestClient restClient = new KsqlRestClient("xxxx", Collections.emptyMap());
  private final Cli.RemoteServerSpecificCommand command = new Cli.RemoteServerSpecificCommand(restClient, new PrintWriter(out));

  @Test
  public void shouldRestClientServerAddressWhenNonEmptyStringArg() {
    command.execute("blah");
    assertThat(restClient.getServerAddress(), equalTo("blah"));
  }

  @Test
  public void shouldPrintServerAddressWhenEmptyStringArg() {
    command.execute("");
    assertThat(out.toString(), equalTo("xxxx\n"));
    assertThat(restClient.getServerAddress(), equalTo("xxxx"));
  }

  @Test
  public void shouldPrintErrorWhenCantConnectToNewAddress() {
    command.execute("blah");
    assertThat(out.toString(),
        containsString("Error issuing GET to KSQL server"));
  }

  @Test
  public void shouldPrintErrorOnErrorResponseFromRestClient() {
    final Cli.RemoteServerSpecificCommand command = new Cli.RemoteServerSpecificCommand(
        new KsqlRestClient("xxxx", Collections.emptyMap()) {
          @Override
          public RestResponse<ServerInfo> getServerInfo() {
            return RestResponse.erroneous(
                Errors.ERROR_CODE_SERVER_ERROR, "it is broken");
          }
        }, new PrintWriter(out));
    command.execute("http://localhost:8088");

    assertThat(out.toString(), containsString("it is broken"));
  }

  @Test
  public void shouldPrintHelp() {
    command.printHelp();
    assertThat(out.toString(), containsString("server:\n\tShow the current server"));
    assertThat(out.toString(), containsString("server <server>:\n\tChange the current server to <server>"));
  }

}