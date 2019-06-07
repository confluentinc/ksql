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
 *    public static Udaf<Long, Long> createSumLong() {
 *      return new TableUdaf<Long, Long>() {
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
 *        public Long aggregate(final Long aggregate, final Long aLong) {
 *          return aggregate + aLong;
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
 *    public static Udaf<Double, Double> createSumDouble() {
 *      return new Udaf<Double, Double>() {
 *        @Override
 *        public Double initialize() {
 *          return 0.0;
 *        }
 *
 *        @Override
 *        public Double aggregate(final Double aggregate, final Double val) {
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
  String description();
}
