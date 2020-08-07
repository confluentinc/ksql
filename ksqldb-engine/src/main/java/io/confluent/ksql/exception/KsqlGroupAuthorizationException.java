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

package io.confluent.ksql.exception;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.GroupAuthorizationException;

/**
 * Used to return custom error messages when TopicAuthorizationException returned from Kafka
 */
public class KsqlGroupAuthorizationException extends GroupAuthorizationException {

  public KsqlGroupAuthorizationException(
      final AclOperation operation,
      final String group) {
    super(String.format("Authorization denied to %s on group: [%s]",
        StringUtils.capitalize(
            operation.toString().toLowerCase()),
        group));
  }

}
