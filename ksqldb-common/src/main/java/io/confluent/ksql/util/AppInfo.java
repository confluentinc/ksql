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

package io.confluent.ksql.util;

import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AppInfo {

  private static final Logger log = LoggerFactory.getLogger(AppInfo.class);
  private static String version = "unknown";
  private static String commitId = "unknown";

  static {
    try {
      final Properties props = new Properties();
      try (InputStream resourceAsStream = AppInfo.class.getResourceAsStream(
          "/ksqldb/version.properties")) {
        props.load(resourceAsStream);
      }
      version = props.getProperty("version", version).trim();
    } catch (final Exception e) {
      log.warn("Error while loading version:", e);
    }

    try {
      final Properties props = new Properties();
      try (InputStream resourceAsStream = AppInfo.class.getResourceAsStream(
          "/git.properties")) {
        props.load(resourceAsStream);
      }
      commitId = props.getProperty("git.commit.id", commitId).trim();
    } catch (final Exception e) {
      log.warn("Error while loading git properties:", e);
    }
  }

  private AppInfo() {
  }

  public static String getVersion() {
    return version;
  }

  public static String getCommitId() {
    return commitId;
  }

  public static void main(final String[] args) {
    System.err.println(getVersion());
    System.err.println(getCommitId());
  }
}
