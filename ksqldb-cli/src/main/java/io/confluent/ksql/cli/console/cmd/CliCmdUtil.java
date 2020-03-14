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

import io.confluent.ksql.util.KsqlException;
import java.util.Collection;

final class CliCmdUtil {

  private CliCmdUtil() {
  }

  static void ensureArgCountBounds(
      final Collection<String> args,
      final int minArgCount,
      final int maxArgCount,
      final String helpMsg
  ) {
    if (args.size() < minArgCount) {
      throw new KsqlException("Too few parameters"
          + System.lineSeparator()
          + helpMsg);
    }

    if (args.size() > maxArgCount) {
      throw new KsqlException("Too many parameters"
          + System.lineSeparator()
          + helpMsg);
    }
  }
}
