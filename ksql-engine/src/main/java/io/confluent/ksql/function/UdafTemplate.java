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

package io.confluent.ksql.function;

import com.google.common.primitives.Primitives;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import io.confluent.ksql.function.udaf.TableUdaf;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.lang.model.element.Modifier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@SuppressWarnings("unused") // Used from generated code
public final class UdafTemplate {

  private UdafTemplate() {
  }

  static String generateCode(
      final Method udafMethod,
      final String className,
      final String udafName,
      final String description) {

    final TypeSpec.Builder udafTypeSpec = TypeSpec.classBuilder(className);
    udafTypeSpec.addModifiers(Modifier.PUBLIC);

    if (TableUdaf.class.isAssignableFrom(udafMethod.getReturnType())) {
      udafTypeSpec.superclass(GeneratedTableAggregateFunction.class);
    } else {
      udafTypeSpec.superclass(GeneratedAggregateFunction.class);
    }

    final String udafArgs = IntStream.range(0, udafMethod.getParameterTypes().length)
        .mapToObj(i -> String.format("(%s) coerce(initArgs, %s.class, %d)",
            Primitives.wrap(udafMethod.getParameterTypes()[i]).getName(),
            udafMethod.getParameterTypes()[i].getName(),
            i))
        .collect(Collectors.joining(", "));

    udafTypeSpec.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(AggregateFunctionInitArguments.class, "initArgs")
            .addParameter(ParameterizedTypeName.get(List.class, Schema.class), "argTypes")
            .addParameter(Schema.class, "aggregateType")
            .addParameter(Schema.class, "outputType")
            .addParameter(ParameterizedTypeName.get(Optional.class, Metrics.class), "metrics")
            .addStatement(
                "super($S, initArgs.udafIndex(), $T.$L($L), aggregateType, "
                    + "outputType, argTypes, $S, metrics)",
                udafName,
                udafMethod.getDeclaringClass(),
                udafMethod.getName(),
                udafArgs,
                description)
            .build());

    udafTypeSpec.addMethod(
        MethodSpec.methodBuilder("getSourceMethodName")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .addStatement("return $S", udafMethod.getName())
            .returns(String.class)
            .build());

    return JavaFile.builder("io.confluent.ksql.function.udaf", udafTypeSpec.build())
        .addStaticImport(UdafTemplate.class, "coerce")
        .build()
        .toString();
  }

  @SuppressWarnings("unchecked")
  public static <T> T coerce(
      final AggregateFunctionInitArguments args,
      final Class<?> clazz,
      final int index) {
    if (Integer.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Integer.valueOf(args.arg(index));
    } else if (Long.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Long.valueOf(args.arg(index));
    } else if (Double.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Double.valueOf(args.arg(index));
    } else if (Float.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Float.valueOf(args.arg(index));
    } else if (Byte.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Byte.valueOf(args.arg(index));
    } else if (Short.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Short.valueOf(args.arg(index));
    } else if (Boolean.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Boolean.valueOf(args.arg(index));
    } else if (String.class.isAssignableFrom(clazz)) {
      return (T) args.arg(index);
    } else if (Struct.class.isAssignableFrom(clazz)) {
      return (T) args.arg(index);
    }

    throw new KsqlFunctionException("Unsupported udaf argument type: " + clazz);
  }

}
