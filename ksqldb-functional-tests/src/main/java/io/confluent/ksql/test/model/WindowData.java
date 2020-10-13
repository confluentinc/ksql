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


package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;

public final class WindowData {

  public enum Type { SESSION, TIME }

  public final long start;
  public final long end;
  public final Type type;

  public WindowData(
      @JsonProperty("start") final long start,
      @JsonProperty("end") final long end,
      @JsonProperty("type") final String type
  ) {
    this.start = start;
    this.end = end;
    this.type = Type.valueOf(requireNonNull(type, "type").toUpperCase());
  }

  public WindowData(final Windowed<?> windowed) {
    this(
        windowed.window().start(),
        windowed.window().end(),
        windowed.window() instanceof SessionWindow
            ? Type.SESSION.toString()
            : Type.TIME.toString()
    );
  }

  public long size() {
    return end - start;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowData that = (WindowData) o;
    return start == that.start
        && end == that.end
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, type);
  }
}