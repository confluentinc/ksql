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

package io.confluent.ksql.statement;

import io.confluent.ksql.parser.tree.Statement;

/**
 * Encapsulates a chain of injectors, ordered, into a single entity.
 */
public final class InjectorChain implements Injector {

  private final Injector[] injectors;

  public static InjectorChain of(final Injector... injectors) {
    return new InjectorChain(injectors);
  }

  private InjectorChain(final Injector... injectors) {
    this.injectors = injectors;
  }

  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    ConfiguredStatement<T> injected = statement;
    for (Injector injector : injectors) {
      injected = injector.inject(injected);
    }
    return injected;
  }
}
