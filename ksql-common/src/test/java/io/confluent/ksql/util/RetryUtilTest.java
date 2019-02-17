/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class RetryUtilTest {
  final Runnable runnable = mock(Runnable.class);
  @SuppressWarnings("unchecked")
  final Consumer<Long> sleep = mock(Consumer.class);

  @Test
  public void shouldReturnOnSuccess() {
    RetryUtil.retryWithBackoff(10, 0, 0, runnable);
    verify(runnable, times(1)).run();
  }

  @Test
  public void shouldBackoffOnFailure() {
    doThrow(new RuntimeException("error")).when(runnable).run();
    try {
      RetryUtil.retryWithBackoff(3, 1, 100, runnable, sleep);
      fail("retry should have thrown");
    } catch (final RuntimeException e) {
    }
    verify(runnable, times(4)).run();
    final InOrder inOrder = Mockito.inOrder(sleep);
    inOrder.verify(sleep).accept((long) 1);
    inOrder.verify(sleep).accept((long) 2);
    inOrder.verify(sleep).accept((long) 4);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRespectMaxWait() {
    doThrow(new RuntimeException("error")).when(runnable).run();
    try {
      RetryUtil.retryWithBackoff(3, 1, 3, runnable, sleep);
      fail("retry should have thrown");
    } catch (final RuntimeException e) {
    }
    verify(runnable, times(4)).run();
    final InOrder inOrder = Mockito.inOrder(sleep);
    inOrder.verify(sleep).accept((long) 1);
    inOrder.verify(sleep).accept((long) 2);
    inOrder.verify(sleep).accept((long) 3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldThrowPassThroughExceptions() {
    doThrow(new IllegalArgumentException("error")).when(runnable).run();
    try {
      RetryUtil.retryWithBackoff(3, 1, 3, runnable, IllegalArgumentException.class);
      fail("retry should have thrown");
    } catch (final IllegalArgumentException e) {
    }
    verify(runnable, times(1)).run();
  }
}
