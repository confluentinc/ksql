/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.util;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper for output KSQL welcome messages to the console.
 */
public final class WelcomeMsgUtils {

  private WelcomeMsgUtils() {
  }

  /**
   * Output a welcome message to the console
   */
  public static void displayWelcomeMessage(
      final int consoleWidth,
      final PrintWriter writer
  ) {
    final String[] lines = {
        "",
        "===========================================",
        "=        _  __ _____  ____  _             =",
        "=       | |/ // ____|/ __ \\| |            =",
        "=       | ' /| (___ | |  | | |            =",
        "=       |  <  \\___ \\| |  | | |            =",
        "=       | . \\ ____) | |__| | |____        =",
        "=       |_|\\_\\_____/ \\___\\_\\______|       =",
        "=                                         =",
        "=  Streaming SQL Engine for Apache Kafka® =",
        "==========================================="
    };

    final String copyrightMsg = "Copyright 2017 Confluent Inc.";

    final Integer logoWidth = Arrays.stream(lines)
        .map(String::length)
        .reduce(0, Math::max);

    // Don't want to display the logo if it'll just end up getting wrapped and looking hideous
    if (consoleWidth < logoWidth) {
      writer.println("KSQL, " + copyrightMsg);
    } else {
      final int paddingChars = (consoleWidth - logoWidth) / 2;
      final String leftPadding = IntStream.range(0, paddingChars)
          .mapToObj(idx -> " ")
          .collect(Collectors.joining());

      Arrays.stream(lines)
          .forEach(line -> writer.println(leftPadding + line));

      writer.println();
      writer.println(copyrightMsg);
    }

    writer.println();
    writer.flush();
  }
}
