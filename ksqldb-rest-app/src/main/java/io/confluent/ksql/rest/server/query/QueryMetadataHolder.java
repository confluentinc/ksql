/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.query;

import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Optional;

/**
 * Holds query metadata from running {@link QueryExecutor}.  This can be inspected by an endpoint
 * to find out what type of query was executed and how to process the result.
 */
public final class QueryMetadataHolder {

  private final Optional<TransientQueryMetadata> pushQueryMetadata;
  private final Optional<StreamPullQueryMetadata> streamPullQueryMetadata;
  private final Optional<ScalablePushQueryMetadata> scalablePushQueryMetadata;
  private final Optional<PullQueryResult> pullQueryResult;

  public static QueryMetadataHolder of(
      final TransientQueryMetadata pushQueryMetadata
  ) {
    return new QueryMetadataHolder(
        Optional.of(pushQueryMetadata), Optional.empty(), Optional.empty(), Optional.empty()
    );
  }

  public static QueryMetadataHolder of(
      final StreamPullQueryMetadata streamPullQueryMetadata
  ) {
    return new QueryMetadataHolder(
        Optional.empty(), Optional.of(streamPullQueryMetadata), Optional.empty(), Optional.empty()
    );
  }

  public static QueryMetadataHolder of(
      final ScalablePushQueryMetadata scalablePushQueryMetadata
  ) {
    return new QueryMetadataHolder(
        Optional.empty(), Optional.empty(), Optional.of(scalablePushQueryMetadata), Optional.empty()
    );
  }

  public static QueryMetadataHolder of(
      final PullQueryResult pullQueryResult
  ) {
    return new QueryMetadataHolder(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(pullQueryResult)
    );
  }

  public static QueryMetadataHolder unhandled() {
    return new QueryMetadataHolder(
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
    );
  }

  private QueryMetadataHolder(
      final Optional<TransientQueryMetadata> pushQueryMetadata,
      final Optional<StreamPullQueryMetadata> streamPullQueryMetadata,
      final Optional<ScalablePushQueryMetadata> scalablePushQueryMetadata,
      final Optional<PullQueryResult> pullQueryResult
  ) {
    this.pushQueryMetadata = pushQueryMetadata;
    this.streamPullQueryMetadata = streamPullQueryMetadata;
    this.scalablePushQueryMetadata = scalablePushQueryMetadata;
    this.pullQueryResult = pullQueryResult;
  }

  public Optional<TransientQueryMetadata> getTransientQueryMetadata() {
    return pushQueryMetadata;
  }

  public Optional<StreamPullQueryMetadata> getStreamPullQueryMetadata() {
    return streamPullQueryMetadata;
  }

  public Optional<ScalablePushQueryMetadata> getScalablePushQueryMetadata() {
    return scalablePushQueryMetadata;
  }

  // Most endoints just want to handle something of this interface, for all push type queries,
  // so it's not necessary to differentiate what type it is.
  public Optional<PushQueryMetadata> getPushQueryMetadata() {
    if (pushQueryMetadata.isPresent()) {
      return pushQueryMetadata.map(queryMetadata -> queryMetadata);
    }
    if (streamPullQueryMetadata.isPresent()) {
      return streamPullQueryMetadata.map(StreamPullQueryMetadata::getTransientQueryMetadata);
    }
    if (scalablePushQueryMetadata.isPresent()) {
      return scalablePushQueryMetadata.map(queryMetadata -> queryMetadata);
    }
    return Optional.empty();
  }

  public Optional<PullQueryResult> getPullQueryResult() {
    return pullQueryResult;
  }
}
