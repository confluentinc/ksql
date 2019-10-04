/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.codegen;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class CodeGenSpec {

  private List<ArgumentSpec> arguments = new ArrayList<>();
  private Map<ColumnRef, String> columnToCodeName = new HashMap<>();
  private ListMultimap<FunctionName, String> functionToCodeName = ArrayListMultimap.create();

  public void addParameter(final ColumnRef columnRef, final Class<?> type, final int colIndex) {
    final String codeName = CodeGenUtil.paramName(arguments.size());
    arguments.add(new ArgumentSpec(
        codeName,
        type,
        OptionalInt.of(colIndex),
        Optional.empty()
    ));
    columnToCodeName.put(columnRef, codeName);
  }

  public void addFunction(final FunctionName functionName, final Kudf function) {
    final String codeName = CodeGenUtil.functionName(functionName, arguments.size());
    arguments.add(new ArgumentSpec(
        codeName,
        function.getClass(),
        OptionalInt.empty(),
        Optional.of(function)
    ));
    functionToCodeName.put(functionName, codeName);
  }

  public String[] argumentNames() {
    return arguments.stream().map(ArgumentSpec::name).toArray(String[]::new);
  }

  public Class[] argumentTypes() {
    return arguments.stream().map(ArgumentSpec::type).toArray(Class[]::new);
  }

  public List<ArgumentSpec> arguments() {
    return ImmutableList.copyOf(arguments);
  }

  public String getCodeName(final ColumnRef columnRef) {
    return columnToCodeName.get(columnRef);
  }

  public String reserveFunctionName(final FunctionName functionName) {
    final List<String> names = functionToCodeName.get(functionName);
    if (names.isEmpty()) {
      throw new KsqlException("Ran out of unique names for: " + functionName);
    }
    return names.remove(0);
  }

  public void resolve(
      final GenericRow row,
      final GenericRowValueTypeEnforcer typeEnforcer,
      final Object[] parameters) {
    for (int paramIdx = 0; paramIdx < arguments.size(); paramIdx++) {
      final ArgumentSpec spec = arguments.get(paramIdx);

      if (spec.colIndex().isPresent()) {
        final int colIndex = spec.colIndex().getAsInt();
        parameters[paramIdx] = typeEnforcer
            .enforceColumnType(colIndex, row.getColumns().get(colIndex));
      } else {
        final int copyOfParamIdxForLambda = paramIdx;
        parameters[paramIdx] = spec.kudf()
            .orElseThrow(() -> new KsqlException(
                "Expected parameter at index "
                    + copyOfParamIdxForLambda
                    + " to be a function, but was "
                    + spec));
      }
    }
  }

  /**
   * Represents either a named reference to a column in a generic row, or
   * a function wrapped in a {@code Kudf}.
   */
  @Immutable
  public static class ArgumentSpec {

    private final String name;
    private final Class<?> type;
    private final OptionalInt columnIndex;
    private final Optional<Kudf> kudf;

    ArgumentSpec(
        final String name,
        final Class<?> type,
        final OptionalInt columnIndex,
        final Optional<Kudf> kudf
    ) {
      this.name = name;
      this.type = type;
      this.columnIndex = columnIndex;
      this.kudf = kudf;
    }

    public String name() {
      return name;
    }

    public Class<?> type() {
      return type;
    }

    public OptionalInt colIndex() {
      return columnIndex;
    }

    public Optional<Kudf> kudf() {
      return kudf;
    }

    @Override
    public String toString() {
      return "ArgumentSpec{"
          + "name='" + name + '\''
          + ", type=" + type
          + ", columnIndex=" + columnIndex
          + ", kudf=" + kudf
          + '}';
    }
  }

}
