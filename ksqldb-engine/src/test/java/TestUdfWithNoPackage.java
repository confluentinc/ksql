/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.FunctionInvoker;
import io.confluent.ksql.function.FunctionLoaderUtils;
import org.junit.Test;

public class TestUdfWithNoPackage {

  @Test
  public void shouldCompileMethodsWithNoPackage() throws Exception {
    // Given:
    final double version = Double.parseDouble(System.getProperty("java.specification.version"));
    if (version < 1.9) {
      assertThat(this.getClass().getPackage(), nullValue());
    } else {
      assertThat(this.getClass().getPackage().getName(), is(""));
    }

    // When:
    // motivated by https://github.com/square/javapoet/pull/723
    final FunctionInvoker udf = FunctionLoaderUtils
        .createFunctionInvoker(getClass().getMethod("udf"));

    // Then:
    assertThat(udf.eval(this), is("udf"));
  }

  public String udf() {
    return "udf";
  }

}
