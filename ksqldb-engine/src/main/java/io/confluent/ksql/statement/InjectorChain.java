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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates a chain of injectors, ordered, into a single entity.
 */
public final class InjectorChain implements InjectorWithSideEffects {

  private final List<Injector> injectors;

  public static InjectorChain of(final Injector... injectors) {
    return new InjectorChain(injectors);
  }

  private InjectorChain(final Injector... injectors) {
    this.injectors = ImmutableList.copyOf(injectors);
  }

  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    ConfiguredStatement<T> injected = statement;
    for (final Injector injector : injectors) {
      injected = injector.inject(injected);
    }
    return injected;
  }

  @Override
  public <T extends Statement> ConfiguredStatementWithSideEffects<T> injectWithSideEffects(
      final ConfiguredStatement<T> statement
  ) {
    ConfiguredStatement<T> injected = statement;
    final List<Object> allSideEffects = new ArrayList<>();
    for (final Injector injector : injectors) {
      if (injector instanceof InjectorWithSideEffects) {
        final ConfiguredStatementWithSideEffects<T> wse =
                ((InjectorWithSideEffects) injector).injectWithSideEffects(injected);
        injected = wse.getStatement();
        allSideEffects.addAll(wse.getSideEffects());
      } else {
        injected = injector.inject(injected);
      }
    }
    return new ConfiguredStatementWithSideEffects<>(injected, allSideEffects);
  }

  @Override
  public <T extends Statement> void revertSideEffects(
      final ConfiguredStatementWithSideEffects<T> statement
  ) {
    for (final Injector injector : injectors) {
      if (injector instanceof InjectorWithSideEffects) {
        ((InjectorWithSideEffects) injector).revertSideEffects(statement);
      }
    }
  }
}
