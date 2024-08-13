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

package io.confluent.ksql.test.util;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import java.lang.Thread.State;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class ThreadTestUtil {

  private static final String SYSTEM_THREAD_GROUP_NAME = "system";

  private ThreadTestUtil() {
  }

  public static ThreadFilterBuilder filterBuilder() {
    return new ThreadFilterBuilder();
  }

  public static ThreadSnapshot threadSnapshot(
      final Predicate<Entry<Thread, StackTraceElement[]>> predicate
  ) {
    final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces().entrySet().stream()
        .filter(predicate)
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    return new ThreadSnapshot(threads, predicate);
  }

  public static String detailsOfNewThreads(
      final Set<Thread> previousThreads,
      final Map<Thread, StackTraceElement[]> currentThreads
  ) {
    return difference(previousThreads, currentThreads).entrySet().stream()
        .sorted(Comparator.comparing(e -> e.getKey().getName()))
        .map(e -> formatThreadInfo(e.getKey(), e.getValue()))
        .collect(Collectors.joining(System.lineSeparator() + System.lineSeparator()));
  }

  public static Map<Thread, StackTraceElement[]> difference(
      final Set<Thread> previousThreads,
      final Map<Thread, StackTraceElement[]> currentThreads
  ) {
    final Map<Thread, StackTraceElement[]> difference = new HashMap<>(currentThreads);
    difference.keySet().removeAll(previousThreads);
    return difference;
  }

  private static String formatThreadInfo(
      final Thread thread,
      final StackTraceElement[] stackTrace
  ) {
    return "New Thead: " + thread.getName() + " (" + thread.getState() + ")"
        + System.lineSeparator()
        + "StackTrace: "
        + System.lineSeparator()
        + Arrays.stream(stackTrace)
        .map(frame -> "\t" + frame)
        .collect(Collectors.joining(System.lineSeparator()));
  }

  public static class ThreadSnapshot {

    private final Map<Thread, StackTraceElement[]> threads;
    private final Predicate<Entry<Thread, StackTraceElement[]>> predicate;

    public ThreadSnapshot(
        final Map<Thread, StackTraceElement[]> threads,
        final Predicate<Entry<Thread, StackTraceElement[]>> predicate
    ) {
      this.threads = requireNonNull(threads, "threads");
      this.predicate = requireNonNull(predicate, "predicate");
    }

    public Map<Thread, StackTraceElement[]> getThreads() {
      return Collections.unmodifiableMap(threads);
    }

    public String detailsOfNewThreads(final ThreadSnapshot previous) {
      return ThreadTestUtil.detailsOfNewThreads(previous.threads.keySet(), threads);
    }

    public void assertSameThreads() {
      // Give threads a chance to die...
      assertThatEventually(
          () -> "Active thread-count is on the up: "
              + "is there new ExecutorService that's not being shutdown somewhere?"
              + System.lineSeparator()
              + threadSnapshot(predicate).detailsOfNewThreads(this),
          () -> difference(threads.keySet(), threadSnapshot(predicate).getThreads())
              .keySet()
              .size(),
          is(0)
      );
    }
  }

  public static final class ThreadFilterBuilder {

    private Predicate<Entry<Thread, StackTraceElement[]>> predicate = e -> true;

    private ThreadFilterBuilder() {
      this.excludeJunitThread()
          .excludeJmxServerThreads()
          .excludeJdkThreads()
          .excludeSystem();
    }

    public ThreadFilterBuilder excludeSystem() {
      filter(e -> e.getKey().getThreadGroup() == null
          || !e.getKey().getThreadGroup().getName().equals(SYSTEM_THREAD_GROUP_NAME));
      return this;
    }

    public ThreadFilterBuilder excludeTerminated() {
      filter(e -> e.getKey().getState() != State.TERMINATED);
      return this;
    }

    public ThreadFilterBuilder nameMatches(final Predicate<String> namePredicate) {
      filter(e -> namePredicate.test(e.getKey().getName()));
      return this;
    }

    public ThreadFilterBuilder filter(final Predicate<Entry<Thread, StackTraceElement[]>> f) {
      predicate = predicate.and(f);
      return this;
    }

    public Predicate<Entry<Thread, StackTraceElement[]>> build() {
      return predicate;
    }

    public ThreadFilterBuilder excludeJunitThread() {
      nameMatches(name -> !name.equals("Time-limited test"));
      return this;
    }

    public ThreadFilterBuilder excludeJmxServerThreads() {
      nameMatches(name -> !name.startsWith("JMX server connection"));
      nameMatches(name -> !name.startsWith("Attach Listener"));
      nameMatches(name -> !name.startsWith("RMI Scheduler"));
      nameMatches(name -> !name.startsWith("RMI TCP Accept"));
      nameMatches(name -> !name.startsWith("RMI TCP Connection"));
      return this;
    }

    public ThreadFilterBuilder excludeJdkThreads() {
      nameMatches(name -> {
        final ImmutableSet<String> jdkThreads =
            ImmutableSet.of("executor-Heartbeat", "executor-Rebalance");
        return !jdkThreads.contains(name);
      });
      return this;
    }
  }
}
