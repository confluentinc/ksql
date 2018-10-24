/*
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
 */

package io.confluent.ksql.function.udaf.window;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Used to handle the special cased {@link WindowStartKudaf} and {@link WindowEndKudaf}.
 */
public final class WindowSelectMapper
    implements ValueMapperWithKey<Windowed<?>, GenericRow, GenericRow> {

  private static final Map<String, Type> WINDOW_FUNCTION_NAMES = ImmutableMap.of(
      WindowStartKudaf.getFunctionName().toUpperCase(), Type.StartTime,
      WindowEndKudaf.getFunctionName().toUpperCase(), Type.EndTime
  );

  private final Map<Integer, Type> windowSelects;

  public WindowSelectMapper(
      final Map<Integer, KsqlAggregateFunction> aggFunctionsByIndex) {
    this.windowSelects = aggFunctionsByIndex.entrySet().stream()
        .filter(e ->
            WINDOW_FUNCTION_NAMES.containsKey(e.getValue().getFunctionName().toUpperCase()))
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> WINDOW_FUNCTION_NAMES.get(e.getValue().getFunctionName().toUpperCase())));
  }

  public boolean hasSelects() {
    return !windowSelects.isEmpty();
  }

  @Override
  public GenericRow apply(final Windowed<?> readOnlyKey, final GenericRow row) {
    final Window window = readOnlyKey.window();

    windowSelects.forEach((index, type) ->
        row.getColumns().set(index, type.mapper.apply(window)));

    return row;
  }

  private enum Type {
    StartTime(Window::start), EndTime(Window::end);

    private final Function<Window, Object> mapper;

    Type(final Function<Window, Object> mapper) {
      this.mapper = mapper;
    }
  }
}
