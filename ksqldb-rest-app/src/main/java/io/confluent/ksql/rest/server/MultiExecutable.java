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

import java.util.Objects;

/**
 * {@code MultiExecutable} wraps multiple {@code Executable}s and ensures that when
 * an action is called all internal executables will perform the action, regardless
 * of whether or not previous executables succeeded.
 *
 * <p>The executables will be started, stopped and joined in the order that they
 * are supplied in {@link #of(Executable...)}.</p>
 */
public final class MultiExecutable implements Executable  {

  private final Executable[] executables;

  public static Executable of(final Executable... executables) {
    return new MultiExecutable(executables);
  }

  private MultiExecutable(final Executable... executables) {
    this.executables = Objects.requireNonNull(executables, "executables");
  }

  @Override
  public void startAsync() throws Exception {
    doAction(Executable::startAsync);
  }

  @Override
  public void shutdown() throws Exception {
    doAction(Executable::shutdown);
  }

  @Override
  public void notifyTerminated() {
    doAction(Executable::notifyTerminated);
  }

  @Override
  public void awaitTerminated() throws InterruptedException {
    doAction(Executable::awaitTerminated);
  }

  @SuppressWarnings("unchecked")
  private <T extends Exception> void doAction(
      final ExceptionalConsumer<Executable, T> action
  ) throws T {

    T exception = null;
    for (final Executable executable : executables) {
      try {
        action.accept(executable);
      } catch (final Exception e) {
        if (exception == null) {
          exception = (T) e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }

    if (exception != null) {
      throw exception;
    }
  }

  @FunctionalInterface
  private interface ExceptionalConsumer<I, T extends Exception> {
    void accept(I value) throws T;
  }
}
