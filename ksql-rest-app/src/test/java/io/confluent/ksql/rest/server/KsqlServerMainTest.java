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

package io.confluent.ksql.rest.server;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.util.KsqlServerException;
import java.io.File;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class KsqlServerMainTest {
  private KsqlServerMain main;

  @Mock(MockType.NICE)
  private Executable executable;

  private final File mockStreamsStateDir = mock(File.class);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    main = new KsqlServerMain(executable);
    when(mockStreamsStateDir.exists()).thenReturn(true);
    when(mockStreamsStateDir.isDirectory()).thenReturn(true);
    when(mockStreamsStateDir.canWrite()).thenReturn(true);
    when(mockStreamsStateDir.getPath()).thenReturn("/var/lib/kafka-streams");
  }

  @Test
  public void shouldStopAppOnJoin() throws Exception {
    // Given:
    executable.stop();
    expectLastCall();
    replay(executable);

    // When:
    main.tryStartApp();

    // Then:
    verify(executable);
  }

  @Test
  public void shouldStopAppOnErrorStarting() throws Exception {
    // Given:
    executable.start();
    expectLastCall().andThrow(new RuntimeException("Boom"));

    executable.stop();
    expectLastCall();
    replay(executable);

    try {
      // When:
      main.tryStartApp();
      fail();
    } catch (final Exception e) {
      // Expected
    }

    // Then:
    verify(executable);
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryDoesNotExist() {
    // Given:
    when(mockStreamsStateDir.exists()).thenReturn(false);

    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "The kafka streams state directory does not exist: /var/lib/kafka-streams\n"
            + " Make sure the directory exists and is writable for KSQL server.");

    // When:
    KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir);

  }

  @Test
  public void shouldFailIfStreamsStateDirectoryIsNotDirectory() {
    // Given:
    when(mockStreamsStateDir.exists()).thenReturn(false);

    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "The kafka streams state directory does not exist: /var/lib/kafka-streams\n"
            + " Make sure the directory exists and is writable for KSQL server.");

    // When:
    KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir);
  }

  @Test
  public void shouldFailIfStreamsStateDirectoryIsNotWritable() {
    // Given:
    when(mockStreamsStateDir.canWrite()).thenReturn(false);

    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "The kafka streams state directory is not writable for KSQL server:"
            + " /var/lib/kafka-streams\n"
            + " Make sure KSQL server has write access to this directory or change it to a writable"
            + " one by setting `ksql.streams.state.dir` config in the properties file.");

    // When:
    KsqlServerMain.enforceStreamStateDirAvailability(mockStreamsStateDir);
  }
}