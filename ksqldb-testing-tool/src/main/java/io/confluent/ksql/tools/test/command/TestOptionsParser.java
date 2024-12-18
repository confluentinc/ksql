/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.tools.test.command;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import java.io.IOException;
import java.util.Objects;

public final class TestOptionsParser {

  private TestOptionsParser() {

  }

  public static <T> T parse(final String[] args, final Class<T> testOptionsClass)
      throws IOException {
    Objects.requireNonNull(args, "args");
    Objects.requireNonNull(testOptionsClass, "testOptionsClass");
    final SingleCommand<T> testOptionsParser = SingleCommand.singleCommand(testOptionsClass);

    if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
      Help.help(testOptionsParser.getCommandMetadata());
      return null;
    }

    try {
      return testOptionsParser.parse(args);
    } catch (final ParseException exception) {
      if (exception.getMessage() != null) {
        System.err.println(exception.getMessage());
      } else {
        System.err.println("Options parsing failed for an unknown reason");
      }
      System.err.println("See the -h or --help flags for usage information");
    }
    return null;
  }
}
