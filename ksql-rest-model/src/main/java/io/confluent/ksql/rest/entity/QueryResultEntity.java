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

package io.confluent.ksql.rest.entity;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryResultEntity extends KsqlEntity {

  private final Optional<WindowType> windowType;
  private final LogicalSchema schema;
  private final ImmutableList<Row> rows;

  public QueryResultEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("windowType") final Optional<WindowType> windowType,
      @JsonProperty("schema") final LogicalSchema schema,
      @JsonProperty("rows") final List<Row> rows
  ) {
    super(statementText);
    this.windowType = requireNonNull(windowType, "windowType");
    this.schema = requireNonNull(schema, "schema");
    this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows"));

    rows.forEach(row -> row.validate(windowType, schema));
  }

  public Optional<WindowType> getWindowType() {
    return windowType;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public List<Row> getRows() {
    return rows;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResultEntity)) {
      return false;
    }
    final QueryResultEntity that = (QueryResultEntity) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(rows, that.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, rows);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Window {

    private final long start;
    private final OptionalLong end;

    public Window(
        @JsonProperty("start") final long start,
        @JsonProperty("end") final OptionalLong end
    ) {
      this.start = start;
      this.end = requireNonNull(end, "end");
    }

    public long getStart() {
      return start;
    }

    public OptionalLong getEnd() {
      return end;
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
      return start == window.start
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

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Row {

    private final Optional<Window> window;
    private final Map<String, ?> key;
    private final Optional<Map<String, ?>> value;

    @JsonCreator
    public Row(
        @JsonProperty("window") final Optional<Window> window,
        @JsonProperty("key") final LinkedHashMap<String, ?> key,
        @JsonProperty("value") final LinkedHashMap<String, ?> value
    ) {
      this.window = requireNonNull(window, "window");
      this.key = new LinkedHashMap<>(requireNonNull(key, "key"));
      this.value = Optional.ofNullable(value).map(LinkedHashMap::new);
    }

    public Optional<Window> getWindow() {
      return window;
    }

    public Map<String, ?> getKey() {
      return Collections.unmodifiableMap(key);
    }

    public Map<String, ?> getValue() {
      return value
          .map(Collections::unmodifiableMap)
          .orElse(null);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Row that = (Row) o;
      return Objects.equals(this.window, that.window)
          && Objects.equals(this.key, that.key)
          && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(window, key, value);
    }

    void validate(
        final Optional<WindowType> windowType,
        final LogicalSchema schema
    ) {
      if (window.isPresent() != windowType.isPresent()) {
        throw new IllegalArgumentException("window mismatch."
            + " expected: " + windowType
            + ", got: " + window
        );
      }

      window.ifPresent(w -> {
        final boolean rowIsSession = w.end.isPresent();
        final boolean schemaIsSession = windowType.get() == WindowType.SESSION;
        if (rowIsSession != schemaIsSession) {
          throw new IllegalArgumentException("window mismtach."
              + " expected: " + schemaIsSession
              + ", got: " + rowIsSession
          );
        }
      });

      if (schema.key().size() != key.size()) {
        throw new IllegalArgumentException("key field count mismatch."
            + " expected: " + schema.key().size()
            + ", got: " + key.size()
        );
      }

      value.ifPresent(v -> {
        if (schema.value().size() != v.size()) {
          throw new IllegalArgumentException("value field count mismatch."
              + " expected: " + schema.value().size()
              + ", got: " + v.size()
          );
        }
      });
    }
  }
}
