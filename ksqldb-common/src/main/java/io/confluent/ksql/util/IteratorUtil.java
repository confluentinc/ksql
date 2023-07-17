/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class IteratorUtil {

  private IteratorUtil() {}

  public static <T> Iterator<T> onComplete(final Iterator<T> iterator, final Runnable runnable) {
    return new IteratorWithCallbacks<T>(iterator, runnable);
  }

  public static <T> Iterator<T> of(final T... elements) {
    return ImmutableList.copyOf(elements).iterator();
  }

  private static class IteratorWithCallbacks<T> implements Iterator<T> {

    private final Iterator<T> backingIterator;
    private final Runnable completeRunnable;
    private boolean completeCalled = false;

    IteratorWithCallbacks(
        final Iterator<T> backingIterator,
        final Runnable completeRunnable
    ) {
      this.backingIterator = backingIterator;
      this.completeRunnable = completeRunnable;
    }

    @Override
    public boolean hasNext() {
      if (completeCalled) {
        return false;
      }
      final boolean hasNext = backingIterator.hasNext();
      maybeCallComplete(hasNext);
      return hasNext;
    }

    @Override
    public T next() {
      try {
        return backingIterator.next();
      } catch (NoSuchElementException e) {
        maybeCallComplete(false);
        throw e;
      }
    }

    private void maybeCallComplete(final boolean hasNext) {
      if (!hasNext && !completeCalled) {
        completeCalled = true;
        completeRunnable.run();
      }
    }
  }
}
