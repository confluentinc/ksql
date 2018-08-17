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

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public class ThreadLocalCloseable<T extends Closeable> implements Closeable {
  private final List<T> created;
  final ThreadLocal<T> local;

  protected ThreadLocalCloseable(final Supplier<T> initialValueSupplier) {
    this.created = new LinkedList<>();
    this.local = new ThreadLocal<T>() {
      @Override
      protected synchronized T initialValue() {
        created.add(initialValueSupplier.get());
        return created.get(created.size() - 1);
      }
    };
  }

  public T get() {
    return local.get();
  }

  @Override
  public void close() {
    for (final Closeable c : created) {
      try {
        c.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
