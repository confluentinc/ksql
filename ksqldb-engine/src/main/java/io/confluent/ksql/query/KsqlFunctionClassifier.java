/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.query;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.query.QueryError.Type;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code KsqlFunctionClassifier} classifies ksql function exceptions as user error
 */
public class KsqlFunctionClassifier implements QueryErrorClassifier {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlFunctionClassifier.class);

  private final String queryId;

  public KsqlFunctionClassifier(final String queryId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  @Override
  public Type classify(final Throwable e) {
    Type type = Type.UNKNOWN;
    if (e instanceof KsqlFunctionException
        || (e instanceof StreamsException
        && ExceptionUtils.getRootCause(e) instanceof KsqlFunctionException)) {
      type = Type.USER;
    }

    if (type == Type.USER) {
      LOG.info(
          "Classified error as USER error based on invalid user input. Query ID: {} Exception: {}",
          queryId,
          e);
    }

    return type;
  }

}
