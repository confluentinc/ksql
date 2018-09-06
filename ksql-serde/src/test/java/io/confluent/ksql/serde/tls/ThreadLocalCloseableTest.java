/**
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
 **/

package io.confluent.ksql.serde.tls;

import org.easymock.EasyMock;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ThreadLocalCloseableTest {
  @Test
  public void shouldCloseAllInstances() {
    final Object lock = new Object();

    final List<Closeable> closeables = new LinkedList<>();
    final ThreadLocalCloseable<Closeable> testCloseable = new ThreadLocalCloseable<>(
        () -> {
          synchronized (lock) {
            final Closeable closeable = mock(Closeable.class);
            closeables.add(closeable);
            try {
              closeable.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            expectLastCall();
            replay(closeable);
            return closeable;
          }
        }
    );

    final int iterations = 3;
    final List<Thread> threads = new LinkedList<>();
    for (int i = 0; i < iterations; i++) {
      threads.add(new Thread(testCloseable::get));
      threads.get(threads.size() - 1).start();
    }

    threads.forEach(
        t -> {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    testCloseable.close();

    assertThat(closeables.size(), equalTo(iterations));
    closeables.forEach(EasyMock::verify);
  }

  @Test
  public void shouldThrowOnAccessAfterClose() {
    final ThreadLocalCloseable<Closeable> testCloseable = new ThreadLocalCloseable<>(
        () ->  () -> {}
    );
    testCloseable.close();
    try {
      testCloseable.get();
      fail("get() should throw IllegalStateException");
    } catch (final IllegalStateException e) {
    }
  }
}
