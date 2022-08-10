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

package io.confluent.ksql.internal;

import java.util.regex.Pattern;

public final class MetricsTagUtils {

  private MetricsTagUtils() {
  }

  public static final String KSQL_CONSUMER_GROUP_MEMBER_ID_TAG = "member";
  public static final String KSQL_TASK_ID_TAG = "task-id";
  public static final String KSQL_TOPIC_TAG = "topic";
  public static final String KSQL_QUERY_ID_TAG = "query-id";

  public static final Pattern SHARED_RUNTIME_THREAD_PATTERN = Pattern.compile("(.*?)__\\d*_\\d*");

  /*
   For non-shared runtimes, the thread id will look something like this:

   _confluent-ksql-pksqlc-d1m0zquery_ +                  // thread id prefix
   CSAS_TEST_COPY-STREAM_1_23 +                          // query id
   -3d62ddb9-d520-4cb3-9c23-968f8e61e201-StreamThread-1  // thread id suffix
   */
  public static final String UUID_PATTERN =
      "-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
  public static final String THREAD_ID_SUFFIX_PATTERN = UUID_PATTERN + "-StreamThread-\\d";
  public static final String THREAD_ID_PREFIX_PATTERN = "query_|transient_";

  public static final String QUERY_ID_PATTERN = ".*";

  public static final Pattern UNSHARED_RUNTIME_THREAD_PATTERN =
      Pattern.compile(
          // group 1: the lookbehind (?<=) matches the prefix and discards everything before it
          "(?<=" + THREAD_ID_PREFIX_PATTERN + ")"
              // group 2: just matches anything/everything that's between group 1 and group 3
              + "(" + QUERY_ID_PATTERN + ")"
              // group 3: matches the suffix & lookahead to discard the rest (in case we add to it)
              + "(?=(" + THREAD_ID_SUFFIX_PATTERN + "))"
      );

}
