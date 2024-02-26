/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class VariableExecutorTest {
  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  private SessionProperties sessionProperties;

  @Before
  public void setup() {
    sessionProperties = new SessionProperties(
        new HashMap<>(), mock(KsqlHostInfo.class), mock(URL.class), false);
  }

  private void executeDefineVariable(final String sql) {
    final Optional<KsqlEntity> response = CustomExecutors.DEFINE_VARIABLE.execute(
        engine.configure(sql),
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity();
    assertThat(response, is(Optional.empty()));
  }

  private Optional<KsqlEntity> executeUndefineVariable(final String sql) {
    return CustomExecutors.UNDEFINE_VARIABLE.execute(
        engine.configure(sql),
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity();
  }

  @Test
  public void shouldSetVariables() {
    // When:
    executeDefineVariable("DEFINE var1 = 'John Peter';");
    executeDefineVariable("DEFINE var2 = '''John Peter''';");

    // Then:
    final Map<String, String> variablesMap = sessionProperties.getSessionVariables();
    assertThat(variablesMap.size(), is(2));
    assertThat(variablesMap, hasEntry("var1", "John Peter"));
    assertThat(variablesMap, hasEntry("var2", "'John Peter'"));
  }

  @Test
  public void shouldSetCaseInsensitiveVariables() {
    // When:
    executeDefineVariable("DEFINE A = 'val1';");
    executeDefineVariable("DEFINE b = 'val2';");

    // Then:
    final Map<String, String> variablesMap = sessionProperties.getSessionVariables();
    assertThat(variablesMap.containsKey("a"), is(true));
    assertThat(variablesMap.get("a"), is("val1"));
    assertThat(variablesMap.containsKey("A"), is(true));
    assertThat(variablesMap.get("A"), is("val1"));
    assertThat(variablesMap.containsKey("b"), is(true));
    assertThat(variablesMap.get("b"), is("val2"));
    assertThat(variablesMap.containsKey("B"), is(true));
    assertThat(variablesMap.get("B"), is("val2"));
  }

  @Test
  public void shouldUnsetVariables() {
    // Given:
    sessionProperties.setVariable("var1", "1");
    sessionProperties.setVariable("var2", "2");

    // When:
    final Optional<KsqlEntity> response = executeUndefineVariable("UNDEFINE var1;");
    assertThat(response, is(Optional.empty()));

    // Then:
    final Map<String, String> variablesMap = sessionProperties.getSessionVariables();
    assertThat(variablesMap.size(), is(1));
    assertThat(variablesMap, hasEntry("var2", "2"));
  }

  @Test
  public void shouldUnsetCaseInsensitiveVariables() {
    // Given:
    sessionProperties.setVariable("VAR1", "1");

    // When:
    final Optional<KsqlEntity> response = executeUndefineVariable("UNDEFINE var1;");
    assertThat(response, is(Optional.empty()));

    // Then:
    final Map<String, String> variablesMap = sessionProperties.getSessionVariables();
    assertThat(variablesMap.size(), is(0));
  }

  @Test
  public void shouldOverrideCaseInsensitiveVariables() {
    // When:
    executeDefineVariable("DEFINE var1 = '1';");
    executeDefineVariable("DEFINE VAR1 = '2';");
    executeDefineVariable("DEFINE vAr1 = '3';"); // latest update

    // Then:
    final Map<String, String> variablesMap = sessionProperties.getSessionVariables();
    assertThat(variablesMap.size(), is(1));
    assertThat(variablesMap.containsKey("var1"), is(true));
    assertThat(variablesMap.get("var1"), is("3"));
  }

  @Test
  public void shouldReturnWarningWhenUndefineAnUnknownVariable() {
    // When:
    final KsqlEntity response = executeUndefineVariable("UNDEFINE var1;").get();

    // Then:
    assertThat(((WarningEntity)response).getMessage(),
        containsString("Cannot undefine variable 'var1' which was never defined"));
  }

  @Test
  public void shouldThrowOnInvalidValues() {
    // Given:
    final List<String> invalidValues = Arrays.asList(
        "\"3\"",  // double-quotes
        "`3`",    // back-quotes
        "3"       // no quotes
    );

    for (final String invalidValue : invalidValues) {
      // When:
      final ParseFailedException e = assertThrows(
          ParseFailedException.class,
          () -> executeDefineVariable(String.format("DEFINE var1=%s;", invalidValue))
      );

      // Then:
      assertThat(e.getMessage(), containsString("line 1:13: Syntax error at line 1:13"));
      assertThat(e.getUnloggedMessage(), containsString(
          "Syntax Error\n"
              + "Expecting STRING"));
      assertThat(e.getSqlStatement(), containsString("DEFINE var1=" + invalidValue + ";"));
    }
  }
}
