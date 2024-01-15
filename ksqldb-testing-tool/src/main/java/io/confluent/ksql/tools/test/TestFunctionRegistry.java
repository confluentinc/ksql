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

package io.confluent.ksql.tools.test;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;

/**
 * A singleton, initialized, function registry.
 * (Speeds up tests by avoiding loading UDFs in many tests).
 */
public enum TestFunctionRegistry {
  INSTANCE;

  private final transient FunctionRegistry delegate;

  TestFunctionRegistry() {
    final InternalFunctionRegistry mutable = new InternalFunctionRegistry();
    UdfLoaderUtil.load(mutable);
    this.delegate = mutable;
  }

  public FunctionRegistry get() {
    return delegate;
  }
}