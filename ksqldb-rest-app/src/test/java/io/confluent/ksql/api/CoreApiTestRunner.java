/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * Test runner that runs tests defined in the specific test file as well as any inherited test
 * cases annotated with {@code CoreApiTest}.
 */
public class CoreApiTestRunner extends BlockJUnit4ClassRunner {

  public CoreApiTestRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
    if (isInheritedTest(method) && !isCoreApiTest(method)) {
      return;
    }
    super.runChild(method, notifier);
  }

  private boolean isInheritedTest(FrameworkMethod method) {
    return !getTestClass().getName().equals(method.getDeclaringClass().getName());
  }

  private static boolean isCoreApiTest(FrameworkMethod method) {
    return method.getAnnotation(CoreApiTest.class) != null;
  }
}
