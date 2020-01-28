/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matcher;

public class TestUtils {

  public static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
  }

  public static class AsyncAssert {

    private AssertionError error;

    public synchronized <T> void assertAsync(T t,
        Matcher<? super T> expected) {
      try {
        assertThat(t, expected);
      } catch (AssertionError e) {
        error = e;
      }
    }

    public synchronized void throwAssert() {
      if (error != null) {
        throw error;
      }
    }
  }
}
