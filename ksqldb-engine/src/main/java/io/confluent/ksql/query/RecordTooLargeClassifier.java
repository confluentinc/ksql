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

import io.confluent.ksql.query.QueryError.Type;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code RecordTooLargeClassifier} classifies records too large to be produced as user exception.
 */
public class RecordTooLargeClassifier implements QueryErrorClassifier {
  private static final Logger LOG = LoggerFactory.getLogger(RecordTooLargeClassifier.class);

  private final String queryId;

  public RecordTooLargeClassifier(final String queryId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  @Override
  public Type classify(final Throwable e) {
    final Type type = e instanceof StreamsException
                          && ExceptionUtils.getRootCause(e) instanceof RecordTooLargeException
                      ? Type.USER
                      : Type.UNKNOWN;

    if (type == Type.USER) {
      LOG.info(
          "Classified RecordTooLargeException error as USER error. Query ID: {} Exception: {}. "
              + "Consider setting ksql.streams.max.request.size property to a higher value.",
          queryId,
          e.getMessage()
      );
    }

    return type;
  }
}
