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

import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;

public final class CodeGenUtil {

  private static final String PARAM_NAME_PREFIX = "var";
  private static final String SCHEMA_NAME_PREFIX = "schema";

  private CodeGenUtil() {
  }

  public static String paramName(final int index) {
    return PARAM_NAME_PREFIX + index;
  }

  public static String schemaName(final int index) {
    return SCHEMA_NAME_PREFIX + index;
  }

  public static String functionName(final FunctionName fun, final int index) {
    return fun.text() + "_" + index;
  }

  public static String argumentAccessor(final String name,
                                        final SqlType type) {
    final Class<?> javaType = SchemaConverters.sqlToJavaConverter().toJavaType(type);
    return argumentAccessor(name, javaType);
  }

  public static String argumentAccessor(final String name,
                                        final Class<?> type) {
    return String.format("((%s) arguments.get(\"%s\"))", type.getCanonicalName(), name);
  }

}
