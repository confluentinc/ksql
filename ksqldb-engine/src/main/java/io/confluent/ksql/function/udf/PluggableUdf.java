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

import io.confluent.ksql.function.FunctionInvoker;
import io.confluent.ksql.security.ExtensionSecurityManager;
import java.util.Objects;

/**
 * Class to allow conversion from Kudf to UdfInvoker.
 * This may change if we ever get rid of Kudf. As it stands we need
 * to do a conversion from custom UDF -> Kudf, so we can support strong
 * typing etc.
 */
public class PluggableUdf implements Kudf {

  private final FunctionInvoker udf;
  private final Object actualUdf;

  public PluggableUdf(
      final FunctionInvoker udfInvoker,
      final Object actualUdf
  ) {
    this.udf = Objects.requireNonNull(udfInvoker, "udfInvoker");
    this.actualUdf = Objects.requireNonNull(actualUdf, "actualUdf");
  }

  @Override
  public Object evaluate(final Object... args) {
    try {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      return udf.eval(actualUdf, args);
    } finally {
      ExtensionSecurityManager.INSTANCE.popOutUdf();
    }
  }

}
