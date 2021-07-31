/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamsList extends KsqlEntity {

  private final ImmutableList<Stream> streams;

  @JsonCreator
  public StreamsList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("streams") final Collection<SourceInfo.Stream> streams
  ) {
    super(statementText);
    this.streams = ImmutableList.copyOf(streams);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streams is ImmutableList")
  public List<SourceInfo.Stream> getStreams() {
    return streams;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamsList)) {
      return false;
    }
    final StreamsList that = (StreamsList) o;
    return Objects.equals(getStreams(), that.getStreams());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStreams());
  }
}
