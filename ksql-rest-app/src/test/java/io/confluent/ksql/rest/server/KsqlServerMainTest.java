/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.rest.server;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author andy
 * created 19/04/2018
 */
@RunWith(EasyMockRunner.class)
public class KsqlServerMainTest {
  private KsqlServerMain main;

  @Mock(MockType.NICE)
  private Executable executable;

  @Before
  public void setUp() {
    main = new KsqlServerMain(executable);
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
}