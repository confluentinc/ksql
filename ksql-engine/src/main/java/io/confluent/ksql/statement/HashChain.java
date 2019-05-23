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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * A mechanism that can detect whether two KSQL engines are logically equivalent
 * to one another. As of now, a checksum is computed by chaining the hashes of
 * all statements that were successfully executed. Any other information that
 * may cause engines to behave differently should be included in this checksum.
 */
public class HashChain {

  private final AtomicReference<Checksum> current;

  /**
   * Initializes an empty hash chain.
   */
  public HashChain() {
    this(new Checksum());
  }

  private HashChain(final Checksum initial) {
    current = new AtomicReference<>(initial);
  }

  /**
   * Updates the hash chain to reflect the execution of {@code statement}
   *
   * @param statement the statement that was just executed
   */
  public void update(final ConfiguredStatement<?> statement) {
    // convert anything with indeterminate hashCodes to objects we know are
    // well behaved (e.g. java.lang.Enum#hashCode can vary across different
    // JVMs)
    final Map<String, String> serializedOverrides
        = Maps.transformValues(statement.getOverrides(), Object::toString);
    current.updateAndGet(check -> {
      if (statement.getChecksum().map(check::equals).orElse(true)) {
        return new Checksum(
            Objects.hash(
                check,
                statement.getStatementText(),
                serializedOverrides));
      }
      throw new KsqlChecksumException(check, statement);
    });
  }

  /**
   * @return the current, immutable, hash chain root
   */
  public @Nonnull Checksum getChecksum() {
    return current.get();
  }

  /**
   * @return a copy of {@code this}; any changes made to the copy will not
   *         affect the original hash chain
   */
  public HashChain copy() {
    return new HashChain(current.get());
  }

}
