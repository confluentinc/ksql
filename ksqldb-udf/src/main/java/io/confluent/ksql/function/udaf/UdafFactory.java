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

package io.confluent.ksql.function.udaf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
/*
 * This is applied to static methods of classes that are annotated with {@link UdafDescription}.
 * The annotated method must return an implementation of {@link Udaf} or {@link TableUdaf}.
 * For example:
 * <pre>
 *   {@code
 *   @UdafDescription(name = "my_udaf", description = "example udaf")
 *   public class MyUdaf {
 *
 *    @UdafFactory(description = "sums longs")
 *    public static Udaf<Long, Long, Long> createSumLong() {
 *      return new TableUdaf<Long, Long, Long>() {
 *        @Override
 *        public Long undo(final Long valueToUndo, final Long aggregateValue) {
 *          return aggregateValue - valueToUndo;
 *        }
 *
 *        @Override
 *        public Long initialize() {
 *          return 0L;
 *        }
 *
 *        @Override
 *        public Long aggregate(final Long value, final Long aggregate) {
 *          return aggregate + value;
 *        }
 *
 *        @Override
 *        public Long merge(final Long aggOne, final Long aggTwo) {
 *          return aggOne + aggTwo;
 *        }
 *      };
 *    }
 *
 *    @UdafFactory(description = "sums double")
 *    public static Udaf<Double, Double, Double> createSumDouble() {
 *      return new Udaf<Double, Double, Double>() {
 *        @Override
 *        public Double initialize() {
 *          return 0.0;
 *        }
 *
 *        @Override
 *        public Double aggregate(final Double val, final Double aggregate) {
 *          return aggregate + val;
 *        }
 *
 *        @Override
 *        public Double merge(final Double aggOne, final Double aggTwo) {
 *          return aggOne + aggTwo;
 *        }
 *      };
 *    }
 *   }
 *  }
 * </pre>
 */
public @interface UdafFactory {

  /**
   * @return a description for the UDAF
   */
  String description();

  /**
   * The schemas for the parameters.
   *
   * <p>For simple parameters, this is optional and can be determined from
   * the Java value itself. For complex input types (e.g. {@code Struct} types),
   * this is required and will fail if not supplied.
   *
   * <p>If you provide fewer schemas than there are actual parameters, the schemas
   * for the remaining parameters will default to being empty.
   */
  String[] paramSchema() default "";

  /**
   * The schema for the intermediate aggregate value.
   *
   * <p>For simple parameters, this is optional and can be determined from
   * the Java value itself. For complex aggregate types (e.g. {@code Struct} types),
   * this is required and will fail if not supplied.
   */
  String aggregateSchema() default "";

  /**
   * The schema for the return value.
   *
   * <p>For simple parameters, this is optional and can be determined from
   * the Java value itself. For complex return types (e.g. {@code Struct} types),
   * this is required and will fail if not supplied.
   */
  String returnSchema() default "";
}
