/*
 * Copyright 2020 Confluent Inc.
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
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MissingTopicClassifier} classifies missing source topic exceptions as user error
 */
public class MissingTopicClassifier implements QueryErrorClassifier {

  private static final Logger LOG = LoggerFactory.getLogger(MissingTopicClassifier.class);

  private final String queryId;

  public MissingTopicClassifier(final String queryId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  @Override
  public Type classify(final Throwable e) {
    final Type type = e instanceof MissingSourceTopicException ? Type.USER : Type.UNKNOWN;

    if (type == Type.USER) {
      LOG.info(
          "Classified error as USER error based on missing topic. Query ID: {} Exception: {}",
          queryId,
          e);
    }

    return type;
  }

}
