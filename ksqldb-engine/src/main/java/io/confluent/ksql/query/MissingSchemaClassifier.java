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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;

/**
 * {@code MissingSchemaClassifier} classifies missing SR schema exceptions as user error
 */
public class MissingSchemaClassifier implements QueryErrorClassifier {
  private static final Logger LOG = LoggerFactory.getLogger(MissingSchemaClassifier.class);

  private final String queryId;

  public MissingSchemaClassifier(final String queryId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  @Override
  public Type classify(final Throwable e) {
    final Type type = SchemaRegistryUtil.isSchemaNotFoundErrorCode(e) ? Type.USER : Type.UNKNOWN;

    if (type == Type.USER) {
      LOG.info(
          "Classified error as USER error based on missing SR schema. Query ID: {} Exception: {}",
          queryId,
          e.getMessage());
    }

    return type;
  }
}
