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

import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.client.KsqlRestClient;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class RemoteCliTest {

  private final KsqlRestClient restClient = new KsqlRestClient("xxxx", Collections.emptyMap());
  private final TestTerminal terminal = new TestTerminal(OutputFormat.TABULAR, restClient);

  @Test
  public void shouldPrintErrorIfCantConnectToRestServer() {
    new RemoteCli(1L, 1L, restClient, terminal);
    assertThat(terminal.getOutputString(), containsString("Remote server address may not be valid"));
  }

  @Test
  public void shouldRegisterRemoteCommand() {
    new RemoteCli(1L, 1L, restClient, terminal);
    assertThat(terminal.getCliSpecificCommands().get("server"),
        instanceOf(RemoteCli.RemoteCliSpecificCommand.class));
  }

}