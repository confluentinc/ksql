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

package io.confluent.ksql.function;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Used to restrict the classes that can be loaded by user supplied UDFs.
 * Parses a file that has a single entry per line. Each entry is a substring of a class or package
 * that should be blacklisted, for example, given a file with the following contents:
 * <pre>
 *   java.lang.Process
 *   java.lang.Runtime
 *   javax
 * </pre>
 * The blacklist produced would be:
 * <pre>
 *   ^(?:java\.lang\.Process|java\.lang\.Runtime|javax)\.?.*$
 * </pre>
 * The above blacklist would mean that any classes beginning with java.lang.Process,
 * java.lang.Runtime, or javax will be in the blacklist.
 * Blank lines and lines beginning with # are ignored.
 */
public class Blacklist implements Predicate<String> {
  private static final Logger logger = LogManager.getLogger(Blacklist.class);
  private static final String BLACKLIST_NONE = "";
  private static final String BLACKLIST_PREFIX = "^(?:";
  private static final String BLACKLIST_SUFFIX = ")\\.?.*$";

  private String blackList = BLACKLIST_NONE;

  Blacklist(final File inputFile) {
    if (!inputFile.exists()) {
      logger.info("Blacklist file: {} not found. No classes will be blacklisted", inputFile);
      return;
    }
    try {
      this.blackList = Files.readLines(inputFile, Charset.forName(StandardCharsets.UTF_8.name()))
          .stream()
          .map(String::trim)
          .filter(line -> !line.isEmpty())
          .filter(line -> !line.startsWith("#"))
          .map(line -> line.replaceAll("\\.", "\\\\."))
          .collect(Collectors.joining("|", BLACKLIST_PREFIX, BLACKLIST_SUFFIX));

      if (this.blackList.equals(BLACKLIST_PREFIX + BLACKLIST_SUFFIX)) {
        this.blackList = BLACKLIST_NONE;
      }
      logger.info("Setting UDF blacklisted classes to: " + blackList);
    } catch (final IOException e) {
      logger.warn("Failed to load resource blacklist from {}"
          + " no classes will be blacklisted", inputFile);
    }
  }

  @Override
  public boolean test(final String resourceName) {
    return resourceName.matches(blackList);
  }
}
