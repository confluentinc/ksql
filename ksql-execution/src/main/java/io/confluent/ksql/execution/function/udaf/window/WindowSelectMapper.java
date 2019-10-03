/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.function.udaf.window;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Used to handle the special cased {WindowStart} and {WindowEnd}.
 */
public final class WindowSelectMapper
    implements ValueMapperWithKey<Windowed<?>, GenericRow, GenericRow> {

  public static final String WINDOW_START_NAME = "WindowStart";
  public static final String WINDOW_END_NAME = "WindowEnd";

  private static final Map<String, Type> WINDOW_FUNCTION_NAMES = ImmutableMap.of(
      WINDOW_START_NAME.toUpperCase(), Type.StartTime,
      WINDOW_END_NAME.toUpperCase(), Type.EndTime
  );

  private final Map<Integer, Type> windowSelects;

  public WindowSelectMapper(
      final int initialUdafIndex,
      final List<KsqlAggregateFunction<?, ?, ?>> functions
  ) {
    final ImmutableMap.Builder<Integer, Type> selectsBuilder = new Builder<>();
    for (int i = 0; i < functions.size(); i++) {
      final String name = functions.get(i).getFunctionName().toUpperCase();
      if (WINDOW_FUNCTION_NAMES.containsKey(name)) {
        selectsBuilder.put(initialUdafIndex + i, WINDOW_FUNCTION_NAMES.get(name));
      }
    }
    windowSelects = selectsBuilder.build();
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
