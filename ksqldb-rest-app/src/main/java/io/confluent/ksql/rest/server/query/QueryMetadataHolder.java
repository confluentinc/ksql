package io.confluent.ksql.rest.server.query;

import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Optional;

public class QueryMetadataHolder {

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
