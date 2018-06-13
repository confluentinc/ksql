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

package io.confluent.ksql.function;

import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

/**
 * Used to restrict the classes that can be loaded by user supplied UDFs
 */
public class Blacklist implements Predicate<String> {
  private static final Logger logger = LoggerFactory.getLogger(Blacklist.class);
  private static final String EMPTY_BLACKLIST = "^(?)\\.?.*$";

  private String blackList;

  Blacklist(final File inputFile) {
    try {
      final StringBuilder builder = new StringBuilder("^(?:");
      Files.readLines(inputFile, Charset.forName(StandardCharsets.UTF_8.name()))
          .forEach(item -> {
            final String trimmed = item.trim();
            if (!(trimmed.isEmpty() || trimmed.startsWith("#"))) {
              builder.append(trimmed.replaceAll("\\.", "\\\\.")).append("|");
            }
          });
      builder.deleteCharAt(builder.length() - 1);
      builder.append(")\\.?.*$");
      this.blackList = builder.toString().equals(EMPTY_BLACKLIST)
          ? ""
          : builder.toString();
      logger.info("Setting UDF blacklisted classes to: " + blackList);
    } catch (IOException e) {
      logger.error("failed to load resource blacklist from " + inputFile
          + " all classes will be blacklisted");
    }
  }

  @Override
  public boolean test(final String resourceName) {
    return blackList == null || resourceName.matches(blackList);
  }
}
