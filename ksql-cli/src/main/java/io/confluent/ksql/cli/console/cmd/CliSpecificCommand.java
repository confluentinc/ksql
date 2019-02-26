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

import java.io.PrintWriter;
import java.util.List;

public interface CliSpecificCommand {

  /**
   * Get the name of the command.
   *
   * <p>This is used to determine if a line entered in the CLI is trying to execute this command.
   * Comparison is case-insensitive. The name can contain spaces.
   *
   * @return get the name of the command.
   */
  String getName();

  /**
   * Get the help message for the command.
   *
   * @return the help message for this command.
   */
  String getHelpMessage();

  /**
   * Execute the command.
   *
   * @param args any additional arguments supplied.
   *             The arguments will already have had any single quotes removed.
   * @param terminal the terminal to write any output to.
   */
  void execute(List<String> args, PrintWriter terminal);
}
