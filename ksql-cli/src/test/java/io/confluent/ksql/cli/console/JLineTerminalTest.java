/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.KsqlTerminal.StatusClosable;
import java.io.File;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.test.TestUtils;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.jline.utils.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JLineTerminalTest {

  @Mock
  private Predicate<String> cliLinePredicate;
  @Mock
  private Function<Terminal, Status> statusFactory;
  @Mock
  private Status statusBar;
  private JLineTerminal terminal;

  @Before
  public void setUp() throws Exception {
    when(statusFactory.apply(any())).thenReturn(statusBar);

    final File historyFile = TestUtils.tempFile();
    terminal = new JLineTerminal(cliLinePredicate, historyFile.toPath(), statusFactory);
  }

  @Test
  public void shouldSetStatusMessage() {
    // When:
    terminal.setStatusMessage("test message");

    // Then:
    verify(statusBar)
        .update(ImmutableList.of(new AttributedString("test message", AttributedStyle.INVERSE)));
  }

  @Test
  public void shouldResetStatusMessage() {
    // Given:
    final StatusClosable closable = terminal.setStatusMessage("test message");
    clearInvocations(statusBar);

    // When:
    closable.close();

    // Then:
    verify(statusBar)
        .update(ImmutableList.of(new AttributedString("", AttributedStyle.DEFAULT)));
  }
}