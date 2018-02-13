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

package io.confluent.ksql.cli.commands;

import io.confluent.common.config.ConfigException;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.RemoteCli;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import org.easymock.EasyMock;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class RemoteTest {

  @Test
  public void shouldThrowConfigExceptionIfOnlyUsernameIsProvided() throws Exception {
    Remote remoteCommand = new Remote();
    remoteCommand.server = "http://foobar";
    remoteCommand.userName = "someone";

    try {
      remoteCommand.getCli();
      fail("should have thrown a ConfigException since only the username was specified");
    } catch (ConfigException e) {
      // good!
    }
  }

  @Test
  public void shouldThrowConfigExceptionIfOnlyPasswordIsProvided() throws Exception {
    Remote remoteCommand = new Remote();
    remoteCommand.server = "http://foobar";

    remoteCommand.password = "hello";

    try {
      remoteCommand.getCli();
      fail("should have thrown a ConfigException since only the password was specified");
    } catch (ConfigException e) {
      // good!
    }
  }

  @Test
  public void shouldReturnRemoteCliWhenBothUsCliTesternameAndPasswordAreProvided() throws Exception {
    Remote remoteCommand = new Remote();
    remoteCommand.server = "http://foobar";
    remoteCommand.versionCheckerAgent = EasyMock.mock(VersionCheckerAgent.class);
    remoteCommand.userName = "somebody";
    remoteCommand.password = "password";
    Cli cli = remoteCommand.getCli();
    assertNotNull(cli);

    assertTrue(cli instanceof RemoteCli);
    RemoteCli remoteCli = (RemoteCli) cli;

    assertTrue(remoteCli.hasUserCredentials());
  }
}
