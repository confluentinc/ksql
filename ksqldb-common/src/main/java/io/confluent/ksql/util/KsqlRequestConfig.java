/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class KsqlRequestConfig extends AbstractConfig {

  public static final String KSQL_REQUEST_CONFIG_PROPERTY_PREFIX = "request.ksql.";

  public static final ConfigDef CURRENT_DEF = buildConfigDef();

  public static final String KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING =
      "request.ksql.query.pull.skip.forwarding";
  public static final boolean KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DEFAULT = false;
  private static final String KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DOC =
      "Controls whether a ksql host forwards a pull query request to another host";

  public static final String KSQL_REQUEST_INTERNAL_REQUEST =
      "request.ksql.internal.request";
  public static final boolean KSQL_REQUEST_INTERNAL_REQUEST_DEFAULT = false;
  private static final String KSQL_REQUEST_INTERNAL_REQUEST_DOC =
      "Indicates whether a KsqlRequest came from another server ";

  public static final String KSQL_DEBUG_REQUEST =
      "request.ksql.debug.request";
  public static final boolean KSQL_DEBUG_REQUEST_DEFAULT = false;
  private static final String KSQL_DEBUG_REQUEST_DOC =
      "Indicates whether a KsqlRequest should contain debugging information.";

  public static final String KSQL_REQUEST_QUERY_PULL_PARTITIONS =
      "request.ksql.query.pull.partition";
  public static final String KSQL_REQUEST_QUERY_PULL_PARTITIONS_DEFAULT = "";
  private static final String KSQL_REQUEST_QUERY_PULL_PARTITIONS_DOC =
      "Indicates which partitions to limit pull queries to.";

  public static final String KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING =
      "request.ksql.query.push.skip.forwarding";
  public static final boolean KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DEFAULT = false;
  private static final String KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DOC =
      "Controls whether a ksql host forwards a push query request to another host";

  public static final String KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN =
      "request.ksql.query.push.continuation.token";
  public static final String KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN_DEFAULT = "";
  private static final String KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN_DOC =
      "A continuation token that can be provided which will start a push query off right at the "
          + "point of that the token was provided.";

  public static final String KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP =
      "request.ksql.query.push.catchup.consumer.group";
  public static final String KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP_DEFAULT = "";
  private static final String KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP_DOC =
      "The consumer group to use with the catchup client";

  private static ConfigDef buildConfigDef() {
    final ConfigDef configDef = new ConfigDef()
        .define(
            KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING,
            Type.BOOLEAN,
            KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DOC
        ).define(
            KSQL_REQUEST_INTERNAL_REQUEST,
            Type.BOOLEAN,
            KSQL_REQUEST_INTERNAL_REQUEST_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_REQUEST_INTERNAL_REQUEST_DOC
        ).define(
            KSQL_DEBUG_REQUEST,
            Type.BOOLEAN,
            KSQL_DEBUG_REQUEST_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_DEBUG_REQUEST_DOC
        ).define(
            KSQL_REQUEST_QUERY_PULL_PARTITIONS,
            Type.LIST,
            KSQL_REQUEST_QUERY_PULL_PARTITIONS_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_REQUEST_QUERY_PULL_PARTITIONS_DOC
        ).define(
            KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING,
            Type.BOOLEAN,
            KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DOC
        ).define(
            KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN,
            Type.STRING,
            KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN_DOC
        ).define(
            KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP,
            Type.STRING,
            KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP_DOC
        );
    return configDef;
  }

  public KsqlRequestConfig(final Map<?, ?> props) {
    super(CURRENT_DEF, props, false);
  }
}
