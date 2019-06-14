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

package io.confluent.ksql.function.udf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
/*
 * Optionally, applied to @Udf function parameters.
 */
public @interface UdfParameter {

  /**
   * The name of the parameter
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   *
   * <p>If your Java class is compiled with the {@code -parameters} compiler flag, then name of the
   * parameter will be inferred from the method declaration and this value can be left at the
   * default. If compiling with out the {@code -parameters} compiler flag, use this property to
   * provide the user with the name of the parameter.
   *
   * @return parameter name.
   */
  String value() default "";

  /**
   * The parameter description.
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   *
   * @return parameter description.
   */
  String description() default "";

  /**
   * The schema for this parameter.
   *
   * <p>For simple parameters, this is optional and can be determined from
   * the Java value itself. For complex return types (e.g. {@code Struct} types),
   * this is required and will fail if not supplied.
   */
  String schema() default "";
}
