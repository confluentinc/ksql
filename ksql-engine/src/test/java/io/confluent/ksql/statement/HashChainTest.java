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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HashChainTest {

  private static final Function<ConfiguredStatement<?>, Void> FOO = x -> null;
  private static final ConfiguredStatement<?> BASIC_STATEMENT = ConfiguredStatement.of(
      PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
      ImmutableMap.of(),
      mock(KsqlConfig.class)
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldStartWithEmptyChecksum() {
    // Given:
    final HashChain hashChain = new HashChain();

    // When:
    final Checksum checksum = hashChain.getChecksum();

    // Then:
    assertThat(checksum, is(new Checksum()));
  }

  @Test
  public void shouldUpdateChecksum() {
    // Given:
    final HashChain hashChain = new HashChain();
    final ConfiguredStatement<?> statement = BASIC_STATEMENT;

    // When:
    hashChain.update(statement, FOO);

    // Then:
    assertThat(
        hashChain.getChecksum(),
        is(new Checksum(Objects.hash(0, statement.getStatementText(), statement.getOverrides()))));
  }

  @Test
  public void shouldUseNonVaryingHash() {
    // Given:
    final HashChain hashChain1 = new HashChain();
    final HashChain hashChain2 = new HashChain();

    final ConfiguredStatement<?> statement = ConfiguredStatement.of(
        PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
        ImmutableMap.of("foo", new EvilHash()),
        mock(KsqlConfig.class)
    );

    // When:
    hashChain1.update(statement, FOO);
    hashChain2.update(statement, FOO);

    // Then:
    assertThat(hashChain1.getChecksum(), is(hashChain2.getChecksum()));
  }

  @Test
  public void shouldIgnoreKsqlConfig() {
    // Given:
    final HashChain hashChain1 = new HashChain();
    final HashChain hashChain2 = new HashChain();

    final ConfiguredStatement<?> statement1 = ConfiguredStatement.of(
        PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED, false))
    );

    final ConfiguredStatement<?> statement2 = ConfiguredStatement.of(
        PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED, true))
    );

    // When:
    hashChain1.update(statement1, FOO);
    hashChain2.update(statement2, FOO);

    // Then:
    assertThat(hashChain1.getChecksum(), is(hashChain2.getChecksum()));
  }

  @Test
  public void shouldThrowIfStatementChecksumIsNotRootChecksum() {
    // Given:
    final HashChain hashChain = new HashChain(new Checksum(321));
    final ConfiguredStatement<?> statement = ConfiguredStatement.of(
        PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
        ImmutableMap.of("foo", new EvilHash()),
        mock(KsqlConfig.class),
        new Checksum(123)
    );

    // Expect:
    expectedException.expect(KsqlChecksumException.class);
    expectedException.expectMessage("Rejecting statement with invalid checksum");

    // When:
    hashChain.update(statement, FOO);
  }

  @Test
  public void shouldThrowBeforeExecutingUpdateFunctionIfChecksumIsInvalid() {
    // Given:
    final HashChain hashChain = new HashChain(new Checksum(321));
    final ConfiguredStatement<?> statement = ConfiguredStatement.of(
        PreparedStatement.of("LIST", new ListProperties(Optional.empty())),
        ImmutableMap.of("foo", new EvilHash()),
        mock(KsqlConfig.class),
        new Checksum(123)
    );

    final AtomicBoolean wasExecuted = new AtomicBoolean(false);
    final Function<ConfiguredStatement<?>, Boolean> foo = x -> wasExecuted.getAndSet(true);

    // When:
    try {
      hashChain.update(statement, foo);
      fail("Expected hashChainUpdate to fail due to mismatch in Checksum");
    } catch (KsqlException ignored) { }

    // Then:
    assertThat(wasExecuted.get(), Matchers.is(false));
  }

  /**
   * A class that never returns the same hash code twice, but always returns
   * the same value for {@link #toString()}
   */
  private static final class EvilHash {
    final static AtomicInteger PREVIOUS_HASH = new AtomicInteger();

    @Override
    public int hashCode() {
      return PREVIOUS_HASH.incrementAndGet();
    }

    @Override
    public boolean equals(final Object obj) {
      return obj == this;
    }

    @Override
    public String toString() {
      return "GoodHash";
    }
  }
}