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

package io.confluent.ksql.cli.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.cli.Options;
import io.confluent.ksql.rest.client.BasicCredentials;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OptionsTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldUseDefaultServerIfNoneSupplied() {
    // When:
    final Options options = parse();

    // Then:
    assertThat(options.getServer(), is("http://localhost:8088"));
  }

  @Test
  public void shouldWorkWithUserSuppliedServer() {
    // When:
    final Options options = parse("custom server");

    // Then:
    assertThat(options.getServer(), is("custom server"));
  }

  @Test
  public void shouldThrowConfigExceptionIfOnlyUsernameIsProvided() {
    // Given:
    final Options options = parse("-u", "joe");

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("You must specify both a username and a password");

    // When:
    options.getUserNameAndPassword();
  }

  @Test
  public void shouldThrowConfigExceptionIfOnlyPasswordIsProvided() {
    // Given:
    final Options options = parse("http://foobar", "-p", "joe");

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("You must specify both a username and a password");

    // When:
    options.getUserNameAndPassword();
  }

  @Test
  public void shouldReturnUserPasswordPairWhenBothProvided() {
    // When:
    final Options options = parse("http://foobar", "-u", "joe", "-p", "pp");

    // Then:
    assertThat(options.getUserNameAndPassword(),
        is(Optional.of(BasicCredentials.of("joe", "pp"))));
  }

  @Test
  public void shouldReturnEmptyOptionWhenUserAndPassNotPresent() {
    // When:
    final Options options = parse();

    // Then:
    assertThat(options.getUserNameAndPassword(), is(Optional.empty()));
  }

  @Test
  public void shouldNotRequirePasswordIfUserNameNotSet() {
    // When:
    final Options options = parse();

    // Then:
    assertThat(options.requiresPassword(), is(false));
  }

  @Test
  public void shouldNotRequirePasswordIfUserNameAndPasswordSupplied() {
    // When:
    final Options options = parse("-u", "joe", "-p", "oo");

    // Then:
    assertThat(options.requiresPassword(), is(false));
  }

  @Test
  public void shouldRequirePasswordIfUserNameSuppliedButNotPassword() {
    // When:
    final Options options = parse("-u", "joe");

    // Then:
    assertThat(options.requiresPassword(), is(true));
  }

  @Test
  public void shouldNotRequirePasswordIfUserNameAndPasswordSuppliedButEmpty() {
    // When:
    final Options options = parse("-u", "joe", "-p", "");

    // Then:
    assertThat(options.requiresPassword(), is(true));
  }

  @Test
  public void shouldNotTrimPasswords() {
    // When:
    final Options options = parse("-u", "joe", "-p", "  ");

    // Then:
    assertThat(options.getUserNameAndPassword().map(BasicCredentials::password),
        is(Optional.of("  ")));
  }

  private static Options parse(final String... args) {
    try {
      final Options parsed = Options.parse(args);
      assertThat(parsed, is(notNullValue()));
      return parsed;
    } catch (final Exception e) {
      throw new AssertionError("Failed to parse options: " + StringUtils.join(args, ","), e);
    }
  }
}
