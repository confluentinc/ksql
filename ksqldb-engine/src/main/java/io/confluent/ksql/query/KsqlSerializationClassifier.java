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
import io.confluent.ksql.serde.KsqlSerializationException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code KsqlSerializationClassifier} classifies serialization exceptions caused due to schema
 * mismatch as user error when topic name doesn't contain a prefix of "_confluent-ksql-" otherwise
 * classifies it as non-user error
 */
public class KsqlSerializationClassifier implements QueryErrorClassifier {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlSerializationClassifier.class);

  private final String queryId;

  public KsqlSerializationClassifier(final String queryId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  private boolean hasInternalTopicPrefix(final Throwable e) {
    final int index = ExceptionUtils.indexOfThrowable(e, KsqlSerializationException.class);
    final KsqlSerializationException kse =
        (KsqlSerializationException) ExceptionUtils.getThrowableList(e).get(index);

    return kse.getTopic().startsWith(ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX);
  }

  @Override
  public Type classify(final Throwable e) {
    Type type = Type.UNKNOWN;

    if (e instanceof KsqlSerializationException
        || (e instanceof StreamsException
            && (ExceptionUtils.indexOfThrowable(e, KsqlSerializationException.class) != -1))) {
      if (!hasInternalTopicPrefix(e)) {
        type = Type.USER;
        LOG.info(
            "Classified error as USER error based on schema mismatch. Query ID: {} Exception: {}",
            queryId,
            e);
      }
    }

    return type;
  }

}
