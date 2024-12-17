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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class CodeGenSpec {

  private final ImmutableList<ArgumentSpec> arguments;
  private final ImmutableMap<ColumnName, String> columnToCodeName;
  private final ImmutableListMultimap<FunctionName, String> functionToCodeName;
  private final ImmutableMap<CreateStructExpression, String> structToCodeName;

  private CodeGenSpec(
      final ImmutableList<ArgumentSpec> arguments,
      final ImmutableMap<ColumnName, String> columnToCodeName,
      final ImmutableListMultimap<FunctionName, String> functionToCodeName,
      final ImmutableMap<CreateStructExpression, String> structToCodeName
  ) {
    this.arguments = arguments;
    this.columnToCodeName = columnToCodeName;
    this.functionToCodeName = functionToCodeName;
    this.structToCodeName = structToCodeName;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "arguments is ImmutableList")
  public List<ArgumentSpec> arguments() {
    return arguments;
  }

  public String getCodeName(final ColumnName columnName) {
    return columnToCodeName.get(columnName);
  }

  public String getUniqueNameForFunction(final FunctionName functionName, final int index) {
    final List<String> names = functionToCodeName.get(functionName);
    if (names.size() <= index) {
      throw new KsqlException("Cannot get name for " + functionName + " " + index + " times");
    }
    return names.get(index);
  }

  public Map<String, Object> resolveArguments(final GenericRow row) {
    final Map<String, Object> resolvedArguments = new HashMap<>(arguments.size());
    for (int paramIdx = 0; paramIdx < arguments.size(); paramIdx++) {
      final String name = arguments.get(paramIdx).name();
      final Object value = arguments.get(paramIdx).resolve(row);
      resolvedArguments.put(name, value);
    }
    return resolvedArguments;
  }

  public String getStructSchemaName(final CreateStructExpression createStructExpression) {
    final String schemaName = structToCodeName.get(createStructExpression);
    if (schemaName == null) {
      throw new KsqlException(
          "Cannot get name for " + ExpressionFormatter.formatExpression(createStructExpression)
      );
    }
    return schemaName;
  }

  static class Builder {

    private final ImmutableList.Builder<ArgumentSpec> argumentBuilder = ImmutableList.builder();
    private final Map<ColumnName, String> columnRefToName = new HashMap<>();
    private final ImmutableListMultimap.Builder<FunctionName, String> functionNameBuilder =
        ImmutableListMultimap.builder();
    private final Map<CreateStructExpression, String> structToSchemaName =
            new HashMap<CreateStructExpression, String>();
    private int argumentCount = 0;
    private int structSchemaCount = 0;

    void addParameter(
        final ColumnName columnName,
        final Class<?> type,
        final int colIndex
    ) {
      final String codeName = CodeGenUtil.paramName(argumentCount++);
      columnRefToName.put(columnName, codeName);
      argumentBuilder.add(new ValueArgumentSpec(codeName, type, colIndex));
    }

    void addFunction(final FunctionName functionName, final Kudf function) {
      final String codeName = CodeGenUtil.functionName(functionName, argumentCount++);
      functionNameBuilder.put(functionName, codeName);
      argumentBuilder.add(new FunctionArgumentSpec(codeName, function.getClass(), function));
    }

    void addStructSchema(final CreateStructExpression struct, final Schema schema) {
      if (structToSchemaName.containsKey(struct)) {
        return;
      }
      final String structSchemaName = CodeGenUtil.schemaName(structSchemaCount++);
      structToSchemaName.put(struct, structSchemaName);
      argumentBuilder.add(new SchemaArgumentSpec(structSchemaName, schema));
    }

    CodeGenSpec build() {
      return new CodeGenSpec(
          argumentBuilder.build(),
          ImmutableMap.copyOf(columnRefToName),
          functionNameBuilder.build(),
          ImmutableMap.copyOf(structToSchemaName)
      );
    }
  }

  /**
   * Represents either a named reference to a column in a generic row, or a function wrapped in a
   * {@code Kudf}.
   */
  @Immutable
  public interface ArgumentSpec {

    String name();

    Class<?> type();

    Object resolve(GenericRow value);
  }

  @Immutable
  private abstract static class BaseArgumentSpec implements ArgumentSpec {

    private final String name;
    private final Class<?> type;

    BaseArgumentSpec(
        final String name,
        final Class<?> type
    ) {
      this.name = requireNonNull(name, "name");
      this.type = requireNonNull(type, "type");
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Class<?> type() {
      return type;
    }
  }

  @Immutable
  private static final class FunctionArgumentSpec extends BaseArgumentSpec {

    @EffectivelyImmutable
    private final Kudf kudf;

    FunctionArgumentSpec(
        final String name,
        final Class<?> type,
        final Kudf kudf
    ) {
      super(name, type);
      this.kudf = requireNonNull(kudf, "kudf");
    }

    @Override
    public Object resolve(final GenericRow value) {
      return kudf;
    }

    @Override
    public String toString() {
      return "FunctionArgumentSpec{"
          + "name='" + name() + '\''
          + ", type=" + type()
          + ", kudf=" + kudf
          + '}';
    }
  }

  @Immutable
  public static final class ValueArgumentSpec extends BaseArgumentSpec {

    private final int columnIndex;

    ValueArgumentSpec(
        final String name,
        final Class<?> type,
        final int columnIndex
    ) {
      super(name, type);
      this.columnIndex = columnIndex;
    }

    @Override
    public Object resolve(final GenericRow value) {
      return value.get(columnIndex);
    }

    @Override
    public String toString() {
      return "ValueArgumentSpec{"
          + "name='" + name() + '\''
          + ", type=" + type()
          + ", columnIndex=" + columnIndex
          + '}';
    }
  }

  @Immutable
  public static final class SchemaArgumentSpec extends BaseArgumentSpec {

    private final ConnectSchema schema;

    SchemaArgumentSpec(
        final String name,
        final Schema schema
    ) {
      super(name, Schema.class);
      this.schema = (ConnectSchema) requireNonNull(schema, "schema").schema();
    }

    @Override
    public Object resolve(final GenericRow value) {
      return schema;
    }

    @Override
    public String toString() {
      return "StructSchemaArgumentSpec{"
          + "name='" + name() + '\''
          + ", type=" + type()
          + ", schema=" + schema
          + '}';
    }
  }
}
