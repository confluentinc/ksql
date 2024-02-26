/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql;

import java.time.Instant;
import java.util.Objects;

/**
 * Pojo for storing window bounds
 */
public final class Window {

  private final Instant start;
  private final Instant end;

  /**
   * Create instance.
   *
   * @param start the start of the window.
   * @param end the end of the window.
   * @return the instance.
   */
  public static Window of(final Instant start, final Instant end) {
    return new Window(start, end);
  }

  private Window(final Instant start, final Instant end) {
    this.start = Objects.requireNonNull(start, "start");
    this.end = Objects.requireNonNull(end, "end");

    if (end.isBefore(start)) {
      throw new IllegalArgumentException("end before start."
          + " start: " + start
          + ", end: " + end
      );
    }
  }

  public Instant start() {
    return Instant.from(start);
  }

  public Instant end() {
    return Instant.from(end);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Window window = (Window) o;
    return Objects.equals(start, window.start)
        && Objects.equals(end, window.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }

  @Override
  public String toString() {
    return "Window{"
        + "start=" + start
        + ", end=" + end
        + '}';
  }
}
