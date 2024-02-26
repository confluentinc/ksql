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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An {@link Injector} that might produce side effects during
 * {@link #injectWithSideEffects(ConfiguredStatement) the injection}
 * and revert those side effects if necessary.
 */
public interface InjectorWithSideEffects extends Injector {

  <T extends Statement> ConfiguredStatementWithSideEffects<T> injectWithSideEffects(
      ConfiguredStatement<T> statement
  );

  <T extends Statement> void revertSideEffects(ConfiguredStatementWithSideEffects<T> statement);

  /**
   * Container for {@link Statement} and side effects produced during injection.

   * @param <T> type of statement
   */
  class ConfiguredStatementWithSideEffects<T extends Statement> {
    private final ConfiguredStatement<T> statement;
    private final List<Object> sideEffects;

    public ConfiguredStatementWithSideEffects(
        final ConfiguredStatement<T> statement,
        final List<Object> sideEffects
    ) {
      this.statement = statement;
      this.sideEffects = new ArrayList<>(sideEffects);
    }

    public ConfiguredStatement<T> getStatement() {
      return statement;
    }

    public List<Object> getSideEffects() {
      return Collections.unmodifiableList(sideEffects);
    }

    public static <T extends Statement> ConfiguredStatementWithSideEffects<T> withNoEffects(
        final ConfiguredStatement<T> statement
    ) {
      return new ConfiguredStatementWithSideEffects<>(statement, Collections.emptyList());
    }
  }

  /**
   * Wrap an injector into {@link InjectorWithSideEffects}.
   */
  static InjectorWithSideEffects wrap(Injector injector) {
    if (injector instanceof InjectorWithSideEffects) {
      return (InjectorWithSideEffects) injector;
    } else {
      return new InjectorWithSideEffects() {

        @Override
        public <T extends Statement> ConfiguredStatementWithSideEffects<T> injectWithSideEffects(
            final ConfiguredStatement<T> statement
        ) {
            return ConfiguredStatementWithSideEffects.withNoEffects(inject(statement));
        }

        @Override
        public <T extends Statement> ConfiguredStatement<T> inject(
            final ConfiguredStatement<T> statement
        ) {
            return injector.inject(statement);
        }

        @Override
        public <T extends Statement> void revertSideEffects(
            final ConfiguredStatementWithSideEffects<T> statement
        ) {
        }
      };
    }
  }
}
