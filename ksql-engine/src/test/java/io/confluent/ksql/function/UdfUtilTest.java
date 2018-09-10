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
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class UdfUtilTest {

  @Test
  public void shouldPassIfArgsAreCorrect() {
    final Object[] args = new Object[] {"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs("Test", args, String.class, Long.class);
  }

  @Test (expected = KsqlException.class)
  public void shouldFailIfTypeIsIncorrect() {
    final Object[] args = new Object[] {"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs("Test", args, String.class, Boolean.class);
  }

  @Test (expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooFew() {
    final Object[] args = new Object[] {"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs("Test", args, String.class, Boolean.class, String.class);
  }

  @Test (expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooMany() {
    final Object[] args = new Object[] {"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs("Test", args, String.class);
  }

  @Test
  public void shouldPassWithNullArgs() {
    final Object[] args = new Object[] {"TtestArg1", null};
    UdfUtil.ensureCorrectArgs("Test", args, String.class, Long.class);
  }

  @Test
  public void shouldHandleSubTypes() {
    final Object[] args = new Object[] {1.345, 55};
    UdfUtil.ensureCorrectArgs("Test", args, Number.class, Number.class);
  }
}
