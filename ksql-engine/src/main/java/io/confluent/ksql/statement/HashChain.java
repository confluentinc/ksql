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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

/**
 * A mechanism that can detect whether two KSQL engines are logically equivalent
 * to one another. As of now, a checksum is computed by chaining the hashes of
 * all statements that were successfully executed. Any other information that
 * may cause engines to behave differently should be included in this checksum.
 *
 * <p>All methods in the class are {@code synchronized} to ensure that updating
 * the checksum and the engine state are done atomically.
 */
public class HashChain {

  @GuardedBy("this")
  private Checksum root;

  /**
   * Initializes an empty hash chain.
   */
  public HashChain() {
    this(new Checksum());
  }

  @VisibleForTesting
  HashChain(final Checksum initial) {
    root = initial;
  }

  /**
   * Updates the hash chain to reflect the execution of {@code execute}
   * parameterized by {@code statement}.
   *
   * @param statement the statement that was just executed
   * @param execute   the update function, which is updated atomically
   */
  public synchronized <T> T update(
      final ConfiguredStatement<?> statement,
      final Function<ConfiguredStatement<?>, T> execute
  ) {
    if (!statement.getChecksum().map(root::equals).orElse(true)) {
      throw new KsqlChecksumException(root, statement);
    }

    final T result = execute.apply(statement);

    // convert anything with indeterminate hashCodes to objects we know are
    // well behaved (e.g. java.lang.Enum#hashCode can vary across different
    // JVMs)
    final Map<String, String> serializedOverrides
        = Maps.transformValues(statement.getOverrides(), Object::toString);
    root = new Checksum(
        Objects.hash(
            root,
            statement.getStatementText(),
            serializedOverrides));

    return result;
  }

  /**
   * @return the current, immutable, hash chain root
   */
  public synchronized @Nonnull Checksum getChecksum() {
    return root;
  }

  /**
   * @return a copy of {@code this}; any changes made to the copy will not
   *         affect the original hash chain
   */
  public synchronized HashChain copy() {
    return new HashChain(root);
  }

}
