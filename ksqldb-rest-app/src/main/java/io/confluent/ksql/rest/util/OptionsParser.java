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

package io.confluent.ksql.rest.util;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import java.io.IOException;

public final class OptionsParser {

  private OptionsParser() {}

  public static <T> T parse(final String[] args, final Class<T> optionsClass) throws IOException {
    final SingleCommand<T> optionsParser = SingleCommand.singleCommand(optionsClass);

    // If just a help flag is given, an exception will be thrown due to missing required options;
    // hence, this workaround
    for (final String arg : args) {
      if ("--help".equals(arg) || "-h".equals(arg)) {
        Help.help(optionsParser.getCommandMetadata());
        return null;
      }
    }

    try {
      return optionsParser.parse(args);
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
