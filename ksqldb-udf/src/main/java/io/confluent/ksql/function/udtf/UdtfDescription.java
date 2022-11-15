/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Classes with this annotation will be scanned for the @Udtf annotation. This tells KSQL that this
 * class contains methods that you would like to add as table functions to KSQL. The name of the
 * function will be the same for each of the @Udtf annotated methods in your class. The parameters
 * and return types can vary.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface UdtfDescription {

  /**
   * The name of the table function.
   *
   * <p>This is the identifier users will use to invoke this table function.
   * The function name must be unique.
   *
   * @return function name.
   */
  String name();

  /**
   * A description of the table function.
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   *
   * <p>If there are multiple overloads of the function implementation, individual overloads can
   * chose to return specific descriptions via {@link Udf#description()}. In which case, both this
   * general description and the overload specific descriptions will be displayed to the user.
   *
   * @return function description.
   */
  String description();

  /**
   * The category or type of the table function.
   *
   * <p>This text is used to group functions displayed when invoking {@code SHOW FUNCTIONS ...}.
   *
   * @return function category.
   */
  String category() default FunctionCategory.TABLE;

  /**
   * The author of the table function.
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   *
   * @return function author.
   */
  String author() default "";

  /**
   * The version of the table function.
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   *
   * @return function version.
   */
  String version() default "";
}
