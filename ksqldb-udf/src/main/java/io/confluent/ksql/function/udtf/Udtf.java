/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udtf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The {@code Udtf} annotation on a method tells KSQL that this method should be exposed
 * as a user-defined table function in KSQL.
 * The enclosing class must also be annotated with {@code UdtfDescription}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Udtf {

  /**
   * The function description.
   *
   * <p>Useful where there are multiple overloaded versions of a function.
   * This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   * @return the text to display to the user.
   */
  String description() default "";

  /**
   * The schema for the return value of the UDTF.
   *
   * <p>For simple method signatures, this is optional and can be determined from
   * the return value itself. For complex return types (e.g. {@code Struct} types),
   * this is required and will fail if not supplied.
   */
  String schema() default "";

  /**
   * The name of the method that provides the return type of the UDTF.
   * @return the name of the other method
   */
  String schemaProvider() default "";
}
