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

import io.confluent.ksql.function.udf.UdfMetadata;
import java.io.File;
import java.util.Optional;

public final class UdfLoaderUtil {
  private UdfLoaderUtil() {}

  public static FunctionRegistry load(final MutableFunctionRegistry functionRegistry) {
    new UserFunctionLoader(
        functionRegistry,
        new File("src/test/resources/udf-example.jar"),
        UdfLoaderUtil.class.getClassLoader(),
        value -> false, Optional.empty(), true
    )
        .load();

    return functionRegistry;
  }

  public static UdfFactory createTestUdfFactory(final KsqlScalarFunction udf) {
    final UdfMetadata metadata = new UdfMetadata(
        udf.name().text(),
        udf.getDescription(),
        "Test Author",
        "",
        FunctionCategory.OTHER,
        KsqlScalarFunction.INTERNAL_PATH
    );

    return new UdfFactory(udf.getKudfClass(), metadata);
  }
}
