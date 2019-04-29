/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import io.confluent.ksql.function.UdfCompiler;
import io.confluent.ksql.function.UdfInvoker;
import java.util.Optional;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUdfWithNoPackage {

  private final UdfCompiler udfCompiler = new UdfCompiler(Optional.empty());

  @BeforeClass
  public static void ensureTestHasNoPackage() {
    // guard against someone accidentally refactoring and moving this test
    // into a non-empty package
    assertThat(TestUdfWithNoPackage.class.getPackage().getName(), is(""));
  }

  @Test
  public void shouldCompileMethodsWithNoPackage() throws Exception {
    // motivated by https://github.com/square/javapoet/pull/723
    final UdfInvoker udf = udfCompiler
        .compile(getClass().getMethod("udf"), this.getClass().getClassLoader());

    assertThat(udf.eval(this), is("udf"));
  }

  public String udf() {
    return "udf";
  }

}
