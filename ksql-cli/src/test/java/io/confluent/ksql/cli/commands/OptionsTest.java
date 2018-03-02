/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli.commands;

import org.junit.Test;

import io.confluent.common.config.ConfigException;
import io.confluent.ksql.cli.Options;

import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class OptionsTest {

  @Test(expected = ConfigException.class)
  public void shouldThrowConfigExceptionIfOnlyUsernameIsProvided() throws Exception {
    final Options options = Options.parse("http://foobar", "-u", "joe");
    options.getUserNameAndPassword();
  }

  @Test(expected = ConfigException.class)
  public void shouldThrowConfigExceptionIfOnlyPasswordIsProvided() throws Exception {
    final Options options = Options.parse("http://foobar", "-p", "joe");
    options.getUserNameAndPassword();
  }

  @Test
  public void shouldReturnUserPasswordPairWhenBothProvided() throws Exception {
    final Options options = Options.parse("http://foobar", "-u", "joe", "-p", "joe");
    assertTrue(options.getUserNameAndPassword().isPresent());
  }

  @Test
  public void shouldReturnEmptyOptionWhenUserAndPassNotPresent() throws Exception {
    final Options options = Options.parse("http://foobar");
    assertFalse(options.getUserNameAndPassword().isPresent());
  }

}
