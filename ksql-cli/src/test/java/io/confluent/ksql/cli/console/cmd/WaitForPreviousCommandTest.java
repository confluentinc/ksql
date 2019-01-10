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

package io.confluent.ksql.cli.console.cmd;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WaitForPreviousCommandTest {

  @Mock
  private Supplier<Boolean> settingSupplier;
  @Mock
  private Consumer<Boolean> settingConsumer;
  private StringWriter out;

  private WaitForPreviousCommand waitForPreviousCommand;

  @Before
  public void setUp() {
    out = new StringWriter();
    waitForPreviousCommand =
        new WaitForPreviousCommand(new PrintWriter(out), settingSupplier, settingConsumer);
  }

  @Test
  public void shouldPrintHelp() {
    // When:
    waitForPreviousCommand.printHelp();

    // Then:
    assertThat(out.toString(), containsString("View the current setting"));
    assertThat(out.toString(), containsString("Update the setting as specified."));
  }

  @Test
  public void shouldPrintCurrentSettingOfOn() {
    // Given:
    when(settingSupplier.get()).thenReturn(true);

    // When:
    waitForPreviousCommand.execute("");

    // Then:
    assertThat(out.toString(),
        containsString(String.format("Current %s configuration: ON", WaitForPreviousCommand.NAME)));
  }

  @Test
  public void shouldPrintCurrentSettingOfOff() {
    // Given:
    when(settingSupplier.get()).thenReturn(false);

    // When:
    waitForPreviousCommand.execute("");

    // Then:
    assertThat(out.toString(),
        containsString(String.format("Current %s configuration: OFF", WaitForPreviousCommand.NAME)));
  }

  @Test
  public void shouldUpdateSettingToOn() {
    // When:
    waitForPreviousCommand.execute("on");

    // Then:
    verify(settingConsumer).accept(true);
  }

  @Test
  public void shouldUpdateSettingToOff() {
    // When:
    waitForPreviousCommand.execute("OFF");

    // Then:
    verify(settingConsumer).accept(false);
  }

  @Test
  public void shouldRejectUpdateOnInvalidSetting() {
    // When:
    waitForPreviousCommand.execute("bad");

    // Then:
    verify(settingConsumer, never()).accept(anyBoolean());
    assertThat(out.toString(),
        containsString(String.format("Invalid %s setting: bad", WaitForPreviousCommand.NAME)));
    assertThat(out.toString(), containsString("Valid options are 'ON' and 'OFF'"));
  }
}