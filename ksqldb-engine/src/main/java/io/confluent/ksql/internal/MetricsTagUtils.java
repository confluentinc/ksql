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

  public static final String KSQL_CONSUMER_GROUP_MEMBER_ID_TAG = "consumer_group_member_id";
  public static final String KSQL_TASK_ID_TAG = "task-id";
  public static final String KSQL_TOPIC_TAG = "topic";
  public static final String KSQL_QUERY_ID_TAG = "query-id";

  public static final Pattern NAMED_TOPOLOGY_PATTERN = Pattern.compile("(.*?)__\\d*_\\d*");
  public static final Pattern QUERY_ID_PATTERN =
      Pattern.compile("(?<=query_|transient_)(.*?)(?=-)");
}
