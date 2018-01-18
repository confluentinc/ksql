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

package io.confluent.ksql;

import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.ksql.cli.commands.Local;
import io.confluent.ksql.cli.commands.Remote;
import io.confluent.ksql.cli.commands.Standalone;
import io.confluent.ksql.util.CommonUtils;

public class Ksql {

  public static void main(String[] args) {
    Runnable runnable = null;
    com.github.rvesse.airline.Cli<Runnable> cli =
        com.github.rvesse.airline.Cli.<Runnable>builder("Cli")
            .withDescription("Kafka Query Language")
            .withDefaultCommand(Help.class)
            .withCommand(Local.class)
            .withCommand(Remote.class)
            .withCommand(Standalone.class)
            .build();

    try {
      runnable = cli.parse(args);
      runnable.run();
    } catch (ParseException exception) {
      if (exception.getMessage() != null) {
        System.err.println(exception.getMessage());
      } else {
        System.err.println("Options parsing failed for an unknown reason");
      }
      System.err.println("See the help command for usage information");
    } catch (Exception e) {
      System.err.println(CommonUtils.getErrorMessageWithCause(e));
    }
    if ((runnable != null) && !(runnable instanceof Standalone)) {
      System.exit(0);
    }

  }
}
