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

package io.confluent.ksql.function;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;

/**
 * This class takes a Method that has been marked with the Udf annotation,
 * generates code for it to be invoked and compiles it into an UdfInvoker instance.
 */
public class UdfCompiler {
  private static final Logger logger = LoggerFactory.getLogger(UdfCompiler.class);

  private static final Map<Class, Function<Integer, String>> typeConverters
      = ImmutableMap.<Class, Function<Integer, String>>builder()
      .put(int.class, index -> typeConversionCode("Integer", index))
      .put(Integer.class, index -> typeConversionCode("Integer", index))
      .put(long.class, index -> typeConversionCode("Long", index))
      .put(Long.class, index -> typeConversionCode("Long", index))
      .put(double.class, index -> typeConversionCode("Double", index))
      .put(Double.class, index -> typeConversionCode("Double", index))
      .put(boolean.class, index -> typeConversionCode("Boolean", index))
      .put(Boolean.class, index -> typeConversionCode("Boolean", index))
      .put(String.class, index -> typeConversionCode("String", index))
      .put(Map.class, index -> typeConversionCode("Map", index))
      .put(List.class, index -> typeConversionCode("List", index))
      .build();

  // Templates used to generate the UDF code
  private static final String genericTemplate =
      "#TYPE arg#INDEX;\n"
          + "if(args[#INDEX] == null) arg#INDEX = null;\n"
          + "else if (args[#INDEX] instanceof #TYPE) arg#INDEX = (#TYPE)args[#INDEX];\n"
          + "else if (args[#INDEX] instanceof String) \n"
          + "   try {\n"
          + "       arg#INDEX = #TYPE.valueOf((String)args[#INDEX]);\n"
          + "   } catch (Exception e) {\n"
          + "     throw new KsqlFunctionException(\"Couldn't coerce string argument'\" "
          + "+ args[#INDEX] + \"' to expected type  #TYPE\");\n"
          + "   }\n";


  private static final String INTEGER_NUMBER_TEMPLATE =
      "else if (args[#INDEX] instanceof Number) arg#INDEX = "
          + "((Number)args[#INDEX]).intValue();\n";

  private static final String NUMBER_TEMPLATE =
      "else if (args[#INDEX] instanceof Number) arg#INDEX = "
          + "((Number)args[#INDEX]).#LC_TYPEValue();\n";

  private static final String THROWS_TEMPLATE =
      "else throw new KsqlFunctionException(\"Type: \" + args[#INDEX].getClass() + \""
          + " is not supported by KSQL UDFS\");";


  UdfInvoker compile(final Method method, final ClassLoader loader) {
    try {
      final IScriptEvaluator scriptEvaluator = createScriptEvaluator(method, loader);
      final String code = generateCode(method);
      return (UdfInvoker) scriptEvaluator.createFastEvaluator(code,
          UdfInvoker.class, new String[]{"thiz", "args"});
    } catch (final KsqlException e) {
      throw e;
    } catch (Exception e) {
      throw new KsqlException("Failed to compile udf wrapper class for "
          + method, e);
    }
  }

  /**
   * Generates code for the given method. Assuming the method has a single Boolean argument
   * the generated code would look like the below. A block of code like this is generated for each
   * of the arguments.
   *<pre>{@code
   * // try and coerce the arguments to the types expected by the UDF
   * Boolean arg0;
   * if(args[0] == null) arg0 = null;
   * else if(args[0] instanceof Boolean) arg0 = (Boolean)args[0];
   * else if(args[0] instanceof String) arg0 = Boolean.valueOf((String)args[0]);
   * else throw new KsqlFunctionException("Type: " + args[0].getClass()
   * + "is not supported by KSQL UDFS");
   *
   * // invoke the udf with the args
   * return ((UdfCompilerTest) thiz).udf(arg0);
   *}</pre>
   * @param method  the UDF to generate the code for
   * @return String representation of the code that should be compiled for the UDF
   */

  private static String generateCode(final Method method) {
    final Class<?>[] params = method.getParameterTypes();

    final String prefix = IntStream.range(0, params.length).mapToObj(i -> {
      final Function<Integer, String> converter = typeConverters.get(params[i]);
      if (converter == null) {
        throw new KsqlException("Type " + params[i] + " is not supported in UDFs");
      }
      return converter.apply(i);
    }).collect(Collectors.joining("\n", "", "\nreturn (("
        + method.getDeclaringClass().getSimpleName()
        + ") thiz)." + method.getName() + "("
    ));

    final String code = IntStream.range(0, params.length).mapToObj(i -> "arg" + i)
        .collect(Collectors.joining(",",
            prefix, ");"));

    logger.debug("generated code for udf method = {}\n{}", method, code);
    return code;
  }

  private static IScriptEvaluator createScriptEvaluator(final Method method,
                                                        final ClassLoader loader) throws Exception {
    final IScriptEvaluator scriptEvaluator
        = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    scriptEvaluator.setClassName(method.getDeclaringClass().getName() + "_" + method.getName());
    final String udfClassName = method.getDeclaringClass().getName();
    scriptEvaluator.setDefaultImports(new String[]{
        "java.util.*",
        "io.confluent.ksql.function.KsqlFunctionException",
        udfClassName,
    });
    scriptEvaluator.setParentClassLoader(loader);
    return scriptEvaluator;
  }


  private static String typeConversionCode(final String type, final int index) {
    if (type.equals("Map") || type.equals("List")) {
      return type + " arg" + index + " = (" + type + ")args[" + index + "];\n";
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(genericTemplate);
    if (type.equals("Integer")) {
      builder.append(INTEGER_NUMBER_TEMPLATE);
    } else if (!type.equals("String") && !type.equals("Boolean")) {
      builder.append(NUMBER_TEMPLATE);
    }
    builder.append(THROWS_TEMPLATE);
    return builder.toString()
        .replaceAll("#TYPE", type)
        .replaceAll("#LC_TYPE", type.toLowerCase())
        .replaceAll("#INDEX", String.valueOf(index));
  }
}
