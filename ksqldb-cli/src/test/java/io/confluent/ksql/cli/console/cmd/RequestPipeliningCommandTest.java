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

package io.confluent.ksql.cli.console.cmd;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestPipeliningCommandTest {

  @Mock
  private Supplier<Boolean> settingSupplier;
  @Mock
  private Consumer<Boolean> settingConsumer;
  private StringWriter out;
  private PrintWriter terminal;

  private RequestPipeliningCommand requestPipeliningCommand;

  @Before
  public void setUp() {
    out = new StringWriter();
    terminal = new PrintWriter(out);
    requestPipeliningCommand = RequestPipeliningCommand.create(settingSupplier, settingConsumer);
  }

  @Test
  public void shouldGetHelp() {
    assertThat(requestPipeliningCommand.getHelpMessage(), containsString("View the current setting"));
    assertThat(requestPipeliningCommand.getHelpMessage(), containsString("Update the setting as specified."));
  }

  @Test
  public void shouldPrintCurrentSettingOfOn() {
    // Given:
    when(settingSupplier.get()).thenReturn(true);

    // When:
    requestPipeliningCommand.execute(Collections.emptyList(), terminal);

    // Then:
    assertThat(out.toString(),
        containsString(String.format("Current %s configuration: ON", RequestPipeliningCommand.NAME)));
  }

  @Test
  public void shouldPrintCurrentSettingOfOff() {
    // Given:
    when(settingSupplier.get()).thenReturn(false);

    // When:
    requestPipeliningCommand.execute(Collections.emptyList(), terminal);

    // Then:
    assertThat(out.toString(),
        containsString(String.format("Current %s configuration: OFF", RequestPipeliningCommand.NAME)));
  }

  @Test
  public void shouldUpdateSettingToOn() {
    // When:
    requestPipeliningCommand.execute(ImmutableList.of("on"), terminal);

    // Then:
    verify(settingConsumer).accept(true);
  }

  @Test
  public void shouldUpdateSettingToOff() {
    // When:
    requestPipeliningCommand.execute(ImmutableList.of("OFF"), terminal);

    // Then:
    verify(settingConsumer).accept(false);
  }

  @Test
  public void shouldRejectUpdateOnInvalidSetting() {
    // When:
    requestPipeliningCommand.execute(ImmutableList.of("bad"), terminal);

    // Then:
    verify(settingConsumer, never()).accept(anyBoolean());
    assertThat(out.toString(),
        containsString(String.format("Invalid %s setting: bad", RequestPipeliningCommand.NAME)));
    assertThat(out.toString(), containsString("Valid options are 'ON' and 'OFF'"));
  }
}