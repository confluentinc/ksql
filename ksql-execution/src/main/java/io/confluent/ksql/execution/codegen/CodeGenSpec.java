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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

@Immutable
public final class CodeGenSpec {

  private final ImmutableList<ArgumentSpec> arguments;
  private final ImmutableMap<ColumnRef, String> columnToCodeName;
  private final ImmutableListMultimap<FunctionName, String> functionToCodeName;

  private CodeGenSpec(
      ImmutableList<ArgumentSpec> arguments, ImmutableMap<ColumnRef, String> columnToCodeName,
      ImmutableListMultimap<FunctionName, String> functionToCodeName
  ) {
    this.arguments = arguments;
    this.columnToCodeName = columnToCodeName;
    this.functionToCodeName = functionToCodeName;
  }

  public String[] argumentNames() {
    return arguments.stream().map(ArgumentSpec::name).toArray(String[]::new);
  }

  public Class[] argumentTypes() {
    return arguments.stream().map(ArgumentSpec::type).toArray(Class[]::new);
  }

  public List<ArgumentSpec> arguments() {
    return arguments;
  }

  public String getCodeName(ColumnRef columnRef) {
    return columnToCodeName.get(columnRef);
  }

  public String getUniqueNameForFunction(FunctionName functionName, int index) {
    List<String> names = functionToCodeName.get(functionName);
    if (names.size() <= index) {
      throw new KsqlException("Cannot get name for " + functionName + " " + index + " times");
    }
    return names.get(index);
  }

  public void resolve(GenericRow row, Object[] parameters) {
    for (int paramIdx = 0; paramIdx < arguments.size(); paramIdx++) {
      ArgumentSpec spec = arguments.get(paramIdx);

      if (spec.colIndex().isPresent()) {
        int colIndex = spec.colIndex().getAsInt();
        parameters[paramIdx] = row.getColumns().get(colIndex);
      } else {
        int copyOfParamIdxForLambda = paramIdx;
        parameters[paramIdx] = spec.kudf()
            .orElseThrow(() -> new KsqlException(
                "Expected parameter at index "
                    + copyOfParamIdxForLambda
                    + " to be a function, but was "
                    + spec));
      }
    }
  }

  static class Builder {

    private final ImmutableList.Builder<ArgumentSpec> argumentBuilder = ImmutableList.builder();
    private final Map<ColumnRef, String> columnRefToName = new HashMap<>();
    private final ImmutableListMultimap.Builder<FunctionName, String> functionNameBuilder =
        ImmutableListMultimap.builder();

    private int argumentCount = 0;

    void addParameter(ColumnRef columnRef, Class<?> type, int colIndex) {
      String codeName = CodeGenUtil.paramName(argumentCount);
      argumentBuilder.add(new ArgumentSpec(
          codeName,
          type,
          OptionalInt.of(colIndex),
          Optional.empty()
      ));
      columnRefToName.put(columnRef, codeName);
      argumentCount++;
    }

    void addFunction(FunctionName functionName, Kudf function) {
      String codeName = CodeGenUtil.functionName(functionName, argumentCount);
      functionNameBuilder.put(functionName, codeName);
      argumentBuilder.add(new ArgumentSpec(
          codeName,
          function.getClass(),
          OptionalInt.empty(),
          Optional.of(function)
      ));
      argumentCount++;
    }

    CodeGenSpec build() {
      return new CodeGenSpec(
          argumentBuilder.build(),
          ImmutableMap.copyOf(columnRefToName),
          functionNameBuilder.build()
      );
    }
  }

  /**
   * Represents either a named reference to a column in a generic row, or a function wrapped in a
   * {@code Kudf}.
   */
  @Immutable
  public static class ArgumentSpec {

    private final String name;
    private final Class<?> type;
    private final OptionalInt columnIndex;
    @EffectivelyImmutable
    private final Optional<Kudf> kudf;

    ArgumentSpec(String name, Class<?> type, OptionalInt columnIndex, Optional<Kudf> kudf) {
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
