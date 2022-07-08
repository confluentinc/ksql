/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;

/**
 * Represents something that knows how to write out a query response
 */
public interface QueryStreamResponseWriter {

  QueryStreamResponseWriter writeMetadata(QueryResponseMetadata metaData);

  QueryStreamResponseWriter writeRow(GenericRow row);

  QueryStreamResponseWriter writeContinuationToken(PushContinuationToken pushContinuationToken);

  QueryStreamResponseWriter writeError(KsqlErrorMessage error);

  QueryStreamResponseWriter writeConsistencyToken(ConsistencyToken consistencyToken);

  QueryStreamResponseWriter writeCompletionMessage(String completionMessage);

  QueryStreamResponseWriter writeLimitMessage();

  void end();

}
