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
   * @return parameter name.
   */
  String value();

  /**
   * The parameter description.
   *
   * <p>This text is displayed when the user calls {@code DESCRIBE FUNCTION ...}.
   * @return parameter description.
   */
  String description() default "";
}
