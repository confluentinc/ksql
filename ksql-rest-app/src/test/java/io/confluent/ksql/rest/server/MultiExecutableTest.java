/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static org.mockito.Mockito.doThrow;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiExecutableTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  Executable executable1;
  @Mock
  Executable executable2;

  private Executable multiExecutable;

  @Before
  public void setup() {
    multiExecutable = MultiExecutable.of(executable1, executable2);
  }

  @Test
  public void shouldStartAll() throws Exception {
    // When:
    multiExecutable.start();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).start();
    inOrder.verify(executable2).start();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldJoinAll() throws Exception {
    // When:
    multiExecutable.join();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).join();
    inOrder.verify(executable2).join();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldStopAll() throws Exception {
    // When:
    multiExecutable.stop();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).stop();
    inOrder.verify(executable2).stop();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldSuppressExceptions() throws Exception {
    // Given:
    doThrow(new RuntimeException("danger executable1!")).when(executable1).start();
    doThrow(new RuntimeException("danger executable2!")).when(executable2).start();

    // Expect:
    expectedException.expectMessage("danger executable1!");
    expectedException.expect(new BaseMatcher<Exception>() {
      @Override
      public void describeTo(final Description description) {
      }

      @Override
      public boolean matches(final Object o) {
        return o instanceof Exception
            && ((Exception) o).getSuppressed()[0].getMessage().equals("danger executable2!");
      }
    });

    // When:
    multiExecutable.start();
  }

}