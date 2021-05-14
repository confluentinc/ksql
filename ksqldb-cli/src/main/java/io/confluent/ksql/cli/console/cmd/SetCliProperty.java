/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.cli.console.cmd;

import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

final class SetCliProperty implements CliSpecificCommand {

  private static final String HELP = "set cli <property> <value>:" + System.lineSeparator()
      + "\tSets a CLI local property. NOTE that this differs from setting a KSQL "
      + "property with \"SET 'property'='value'\" in that it does not affect the server.";

  private final BiConsumer<String, String> setProperty;

  public static SetCliProperty create(final BiConsumer<String, String> setProperty) {
    return new SetCliProperty(setProperty);
  }

  private SetCliProperty(final BiConsumer<String, String> setProperty) {
    this.setProperty = Objects.requireNonNull(setProperty, "setProperty");
  }

  @Override
  public String getName() {
    return "set cli";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 2, 2, getHelpMessage());
    setProperty.accept(args.get(0), args.get(1));
  }
}
