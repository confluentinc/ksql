/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiExecutableTest {

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
    multiExecutable.startAsync();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).startAsync();
    inOrder.verify(executable2).startAsync();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldJoinAll() throws Exception {
    // When:
    multiExecutable.awaitTerminated();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).awaitTerminated();
    inOrder.verify(executable2).awaitTerminated();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotifyAllToShutdown() throws Exception {
    // When:
    multiExecutable.notifyTerminated();

    // Then:
    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).notifyTerminated();
    inOrder.verify(executable2).notifyTerminated();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldStopAll() throws Exception {
    // When:
    multiExecutable.shutdown();

    // Then:
    final InOrder inOrder = Mockito.inOrder(executable1, executable2);
    inOrder.verify(executable1).shutdown();
    inOrder.verify(executable2).shutdown();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldSuppressExceptions() throws Exception {
    // Given:
    doThrow(new RuntimeException("danger executable1!")).when(executable1).startAsync();
    doThrow(new RuntimeException("danger executable2!")).when(executable2).startAsync();

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> multiExecutable.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("danger executable1!"));
    assertThat(e, new BaseMatcher<Exception>() {
      @Override
      public void describeTo(final Description description) {
      }

      @Override
      public boolean matches(final Object o) {
        return o instanceof Exception
            && ((Exception) o).getSuppressed()[0].getMessage().equals("danger executable2!");
      }
    });
  }

}