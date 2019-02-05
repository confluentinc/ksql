/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.primitives.Primitives;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.lang.model.element.Modifier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("unused") // Used from generated code
public final class UdafTemplate {

  private UdafTemplate() { }

  static String generateCode(
      final Method udaf,
      final String className,
      final String udafName,
      final String description) {

    final TypeSpec.Builder udafTypeSpec = TypeSpec.classBuilder(className);
    udafTypeSpec.addModifiers(Modifier.PUBLIC);

    if (TableUdaf.class.isAssignableFrom(udaf.getReturnType())) {
      udafTypeSpec.superclass(GeneratedTableAggregateFunction.class);
    } else {
      udafTypeSpec.superclass(GeneratedAggregateFunction.class);
    }

    udafTypeSpec.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterizedTypeName.get(List.class, Schema.class), "args")
            .addParameter(Schema.class, "returnType")
            .addParameter(ParameterizedTypeName.get(Optional.class, Metrics.class), "metrics")
            .addStatement("super($S, returnType, args, $S, metrics)", udafName, description)
            .build());

    udafTypeSpec.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(Udaf.class, "udaf")
            .addParameter(int.class, "index")
            .addParameter(ParameterizedTypeName.get(List.class, Schema.class), "args")
            .addParameter(Schema.class, "returnType")
            .addParameter(Sensor.class, "aggSensor")
            .addParameter(Sensor.class, "mergeSensor")
            .addStatement(
                "super($S, index, supplier(udaf), returnType, args, $S, aggSensor, mergeSensor)",
                udafName, description)
            .addStatement("this.udaf = udaf")
            .build());

    udafTypeSpec.addMethod(
        MethodSpec.methodBuilder("getSourceMethodName")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .addStatement("return $S", udaf.getName())
            .returns(String.class)
            .build());

    final String udafArgs = IntStream.range(0, udaf.getParameterTypes().length)
        .mapToObj(i -> String.format("(%s) coerce(args, %s.class, %d)",
                                     Primitives.wrap(udaf.getParameterTypes()[i]).getName(),
                                     udaf.getParameterTypes()[i].getName(),
                                     i))
        .collect(Collectors.joining(", "));

    udafTypeSpec.addMethod(
        MethodSpec.methodBuilder("getInstance")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(AggregateFunctionArguments.class, "args", Modifier.FINAL)
            .addStatement("args.ensureArgCount($L, $S)", udaf.getParameters().length + 1, udafName)
            .returns(KsqlAggregateFunction.class)
            .addStatement(
                "return new $L($T.$L($L), args.udafIndex(), getArgTypes(), getReturnType(), "
                    + "aggregateSensor, mergeSensor)",
                className,
                udaf.getDeclaringClass(),
                udaf.getName(),
                udafArgs)
            .build());

    return JavaFile.builder("io.confluent.ksql.function.udaf", udafTypeSpec.build())
        .addStaticImport(UdafTemplate.class, "coerce")
        .build()
        .toString();
  }

  @SuppressWarnings("unchecked")
  public static <T> T coerce(
      final AggregateFunctionArguments args,
      final Class<?> clazz,
      final int index) {
    if (Integer.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Integer.valueOf(args.arg(index + 1));
    } else if (Long.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Long.valueOf(args.arg(index + 1));
    } else if (Double.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Double.valueOf(args.arg(index + 1));
    } else if (Float.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Float.valueOf(args.arg(index + 1));
    } else if (Byte.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Byte.valueOf(args.arg(index + 1));
    } else if (Short.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Short.valueOf(args.arg(index + 1));
    } else if (Boolean.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Boolean.valueOf(args.arg(index + 1));
    } else if (String.class.isAssignableFrom(clazz)) {
      return (T) args.arg(index + 1);
    }

    throw new KsqlFunctionException("Unsupported udaf argument type: " + clazz);
  }

}
