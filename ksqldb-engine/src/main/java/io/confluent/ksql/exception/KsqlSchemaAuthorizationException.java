/*
 * Copyright 2021 Confluent Inc.
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

public class KsqlSchemaAuthorizationException extends RuntimeException {
  public KsqlSchemaAuthorizationException(final AclOperation operation, final String objectName) {
    super(String.format("Authorization denied to %s on Schema Registry subject: [%s]",
        StringUtils.capitalize(
            operation.toString().toLowerCase()),
        objectName));
  }
}
